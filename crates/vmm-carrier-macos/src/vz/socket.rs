use super::{Machine, VzHypervisor};
use objc2::{
    AnyThread, DefinedClass, define_class, msg_send, rc::Retained, runtime::ProtocolObject,
};
use objc2_foundation::{NSObject, NSObjectProtocol};
use objc2_virtualization::{
    VZVirtioSocketConnection, VZVirtioSocketDevice, VZVirtioSocketListener,
    VZVirtioSocketListenerDelegate,
};
use std::{
    io,
    os::fd::{BorrowedFd, OwnedFd},
};
use tokio::{net::UnixStream, sync::mpsc};

pub(super) fn duplicate(connection: &VZVirtioSocketConnection) -> io::Result<OwnedFd> {
    // SAFETY: VZ owns the connection and descriptor throughout this callback.
    // The duplicate is owned by Rust before the framework releases its reference.
    unsafe {
        let fd = connection.fileDescriptor();
        if fd < 0 {
            return Err(io::Error::other("closed VZ connection"));
        }
        BorrowedFd::borrow_raw(fd).try_clone_to_owned()
    }
}
pub(super) fn stream(fd: OwnedFd) -> io::Result<UnixStream> {
    let s = std::os::unix::net::UnixStream::from(fd);
    s.set_nonblocking(true)?;
    UnixStream::from_std(s)
}

define_class!(
    // SAFETY: this NSObject subclass only stores a thread-safe channel sender.
    #[unsafe(super = NSObject)]
    #[name = "AgentVMMCarrierSocketDelegate"]
    #[ivars = mpsc::Sender<OwnedFd>]
    pub(super) struct Delegate;

    // SAFETY: NSObject supplies the protocol's base implementation.
    unsafe impl NSObjectProtocol for Delegate {}
    // SAFETY: VZ calls this selector with live references on the owning VM queue.
    unsafe impl VZVirtioSocketListenerDelegate for Delegate {
        #[unsafe(method(listener:shouldAcceptNewConnection:fromSocketDevice:))]
        fn accept(&self, _: &VZVirtioSocketListener, connection: &VZVirtioSocketConnection, _: &VZVirtioSocketDevice) -> bool {
            duplicate(connection).is_ok_and(|fd| self.ivars().try_send(fd).is_ok())
        }
    }
);

pub struct VsockListener(mpsc::Receiver<OwnedFd>);
impl VsockListener {
    pub async fn accept(&mut self) -> io::Result<UnixStream> {
        stream(
            self.0
                .recv()
                .await
                .ok_or_else(|| io::Error::other("vsock listener closed"))?,
        )
    }
}

impl Machine {
    pub(super) fn socket(&self) -> io::Result<Retained<VZVirtioSocketDevice>> {
        // SAFETY: called on the VM queue and configuration installs a single socket device.
        unsafe { self.vm()?.socketDevices().objectAtIndex(0) }
            .downcast::<VZVirtioSocketDevice>()
            .map_err(|_| io::Error::other("missing vsock device"))
    }
}

impl VzHypervisor {
    pub fn listen_vsock(&mut self, port: u32) -> io::Result<VsockListener> {
        self.on_queue(|m, _| {
            let device = m.socket()?;
            let (tx, rx) = mpsc::channel(32);
            let init = Delegate::alloc().set_ivars(tx);
            // SAFETY: NSObject init initializes the allocated delegate. The listener's weak delegate
            // remains retained by the queue-owned map until replacement, removal, or VM shutdown.
            unsafe {
                let delegate: Retained<Delegate> = msg_send![super(init), init];
                let listener = VZVirtioSocketListener::new();
                listener.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
                device.setSocketListener_forPort(&listener, port);
                m.listeners.insert(port, delegate);
            }
            Ok(VsockListener(rx))
        })
    }
    pub fn remove_vsock_listener(&mut self, port: u32) -> io::Result<()> {
        self.on_queue(|m, _| {
            // SAFETY: removal executes on the queue that owns the socket device.
            unsafe { m.socket()?.removeSocketListenerForPort(port) };
            m.listeners.remove(&port);
            Ok(())
        })
    }
}
