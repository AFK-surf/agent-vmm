use crate::{
    backend::LocalDom0Backend,
    catalog::SavedBoot,
    network::Policy,
    proxy::{ExternalServiceRoute, Target},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddrV4},
};
use vmm_core::{BootIdentity, Error, Result, Scope, ServiceRoute};

#[derive(Default)]
pub(crate) struct Routes {
    pub sources: BTreeMap<Ipv4Addr, BootIdentity>,
    pub local: BTreeMap<String, ServiceRoute>,
    pub external: BTreeMap<String, ExternalServiceRoute>,
}
impl Routes {
    pub fn for_source<'a>(
        &'a self,
        source: &'a BootIdentity,
    ) -> impl Iterator<Item = ExternalServiceRoute> + 'a {
        self.local
            .values()
            .filter(move |r| &r.binding.source == source)
            .map(ExternalServiceRoute::from)
            .chain(
                self.external
                    .values()
                    .filter(move |r| &r.source == source)
                    .cloned(),
            )
    }
}

const HOST: Ipv4Addr = Ipv4Addr::new(100, 127, 255, 1);
impl LocalDom0Backend {
    pub(crate) async fn prepare_network(&self, b: &mut SavedBoot) -> Result<()> {
        let src = self.slot(b)?;
        for m in &b.definition.host_shares {
            let path = self
                .cfg
                .host_shares
                .get(&m.share_id)
                .ok_or(Error::Missing("host share grant"))?;
            let listen = SocketAddrV4::new(
                self.cfg.gateway,
                b.share_ports.get(&m.share_id).copied().unwrap_or(0),
            );
            let port = self
                .shares
                .export(
                    &b.identity.instance_id,
                    &m.share_id,
                    path,
                    m.read_only,
                    src,
                    listen,
                )
                .await?;
            b.share_ports.insert(m.share_id.clone(), port);
        }
        for r in &b.scope.host_socket_rules {
            self.proxies
                .bind(
                    SocketAddrV4::new(HOST, r.port),
                    src,
                    Target::Unix(r.socket_path.clone().into()),
                    0,
                )
                .await?;
        }
        self.catalog().save(b)?;
        self.apply_firewall().await
    }
    async fn apply_firewall(&self) -> Result<()> {
        let mut routes = self.routes.lock().await;
        self.replace_firewall(&mut routes)
    }
    fn replace_firewall(&self, routes: &mut Routes) -> Result<()> {
        let mut policies = BTreeMap::new();
        let mut sources = BTreeMap::new();
        for b in self
            .catalog()
            .boots()?
            .into_iter()
            .filter(|b| !b.retired && !b.retiring)
        {
            let mut local =
                BTreeSet::from([SocketAddrV4::new(self.cfg.gateway, self.cfg.boot_port)]);
            for port in b.share_ports.values() {
                local.insert(SocketAddrV4::new(self.cfg.gateway, *port));
            }
            for rule in &b.scope.host_socket_rules {
                local.insert(SocketAddrV4::new(HOST, rule.port));
            }
            for r in routes.for_source(&b.identity) {
                local.insert(SocketAddrV4::new(r.vip, r.port));
            }
            let ip = self.slot(&b)?;
            if sources.insert(ip, b.identity.clone()).is_some() {
                return Err(Error::Fenced);
            }
            policies.insert(
                ip,
                Policy {
                    mode: b.scope.egress_mode.clone(),
                    dns: (self.cfg.dns_port != 0)
                        .then(|| SocketAddrV4::new(self.cfg.gateway, self.cfg.dns_port)),
                    local_ports: local,
                },
            );
        }
        self.firewall.replace(&policies)?;
        routes.sources = sources;
        Ok(())
    }
    pub(crate) async fn policy(&self, boot: &BootIdentity, scope: &Scope) -> Result<()> {
        let mut b = self.catalog().boot(boot)?;
        if b.retiring || b.retired || b.identity.sandbox.scope_id != scope.id {
            return Err(Error::Fenced);
        }
        if scope.revision < b.scope.revision || scope.generation != b.scope.generation {
            return Err(Error::Fenced);
        }
        for r in &b.scope.host_socket_rules {
            if !scope.host_socket_rules.contains(r) {
                self.proxies
                    .revoke(SocketAddrV4::new(HOST, r.port), self.slot(&b)?, 0)
                    .await?;
            }
        }
        b.scope = scope.clone();
        self.prepare_network(&mut b).await
    }
    pub(crate) async fn bind(&self, r: &ServiceRoute) -> Result<()> {
        let target = self.running_slot(&r.binding.export.boot)?;
        self.install_route(
            r.into(),
            Target::Tcp(SocketAddrV4::new(target, r.binding.export.port)),
            Some(r),
        )
        .await
    }
    pub async fn bind_external_route(
        &self,
        route: ExternalServiceRoute,
        target: Target,
    ) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.install_route(route, target, None).await
    }
    fn running_slot(&self, boot: &BootIdentity) -> Result<Ipv4Addr> {
        let b = self.catalog().boot(boot)?;
        if b.retired
            || b.retiring
            || !self
                .present(&b, &self.inventory()?)?
                .is_some_and(|vm| vm.state == vmm_l4hctl::VmState::Running)
        {
            return Err(Error::Fenced);
        }
        self.slot(&b)
    }
    async fn install_route(
        &self,
        route: ExternalServiceRoute,
        target: Target,
        local: Option<&ServiceRoute>,
    ) -> Result<()> {
        let peer = SocketAddrV4::new(route.vip, route.port);
        if route.vip.octets()[..3] != [100, 127, 254]
            || route.port == 0
            || route.port == self.cfg.rpc_port
        {
            return Err(Error::Invalid("service route endpoint"));
        }
        if self
            .catalog()
            .get::<i64>(&format!("revoked/{}", route.id))?
            .is_some_and(|rev| route.revision <= rev)
        {
            return Err(Error::Fenced);
        }
        let source = self.running_slot(&route.source)?;
        let mut routes = self.routes.lock().await;
        let id = route.id.clone();
        let rev = route.revision;
        if routes.local.get(&id).is_some_and(|old| Some(old) != local)
            || routes
                .external
                .get(&id)
                .is_some_and(|old| local.is_some() || old != &route)
        {
            return Err(Error::Fenced);
        }
        if routes.for_source(&route.source).any(|old| {
            old.id != id
                && ((old.vip, old.port) == (route.vip, route.port)
                    || (!route.virtual_service_name.is_empty()
                        && old
                            .virtual_service_name
                            .eq_ignore_ascii_case(&route.virtual_service_name)))
        }) {
            return Err(Error::Conflict("service endpoint or alias already bound"));
        }
        let existed = routes.local.contains_key(&id) || routes.external.contains_key(&id);
        self.proxies.bind(peer, source, target, rev).await?;
        if let Some(r) = local {
            routes.local.insert(id.clone(), r.clone());
        } else {
            routes.external.insert(id.clone(), route);
        }
        let result = self.replace_firewall(&mut routes).and_then(|()| {
            if local.is_some() {
                self.catalog().put("routes", &routes.local)?;
            }
            Ok(())
        });
        if result.is_err() && !existed {
            self.proxies.revoke(peer, source, rev).await?;
            routes.local.remove(&id);
            routes.external.remove(&id);
            self.replace_firewall(&mut routes)?;
        }
        result
    }
    pub(crate) async fn revoke(&self, id: &str, rev: i64) -> Result<()> {
        let mut routes = self.routes.lock().await;
        let key = format!("revoked/{id}");
        let route = routes
            .external
            .get(id)
            .cloned()
            .or_else(|| routes.local.get(id).map(Into::into));
        let prior = self.catalog().get::<i64>(&key)?.unwrap_or(i64::MIN);
        if let Some(r) = &route {
            if r.revision != rev {
                return Err(Error::Fenced);
            }
            let source = self.slot(&self.catalog().boot(&r.source)?)?;
            self.proxies
                .revoke(SocketAddrV4::new(r.vip, r.port), source, rev)
                .await?;
        }
        let old = routes.local.remove(id);
        if let Err(e) = self
            .catalog()
            .revoke_route(&key, prior.max(rev), &routes.local)
        {
            if let Some(r) = old {
                routes.local.insert(id.to_owned(), r);
            }
            return Err(e);
        }
        routes.external.remove(id);
        self.replace_firewall(&mut routes)
    }
    pub(crate) async fn release_network(&self, b: &SavedBoot) -> Result<()> {
        let routes = {
            let table = self.routes.lock().await;
            table
                .local
                .values()
                .filter(|r| r.binding.export.boot == b.identity || r.binding.source == b.identity)
                .map(|r| (r.binding.id.clone(), r.revision))
                .chain(
                    table
                        .external
                        .values()
                        .filter(|r| r.source == b.identity)
                        .map(|r| (r.id.clone(), r.revision)),
                )
                .collect::<Vec<_>>()
        };
        for (id, rev) in routes {
            self.revoke(&id, rev).await?;
        }
        for r in &b.scope.host_socket_rules {
            self.proxies
                .revoke(SocketAddrV4::new(HOST, r.port), self.slot(b)?, 0)
                .await?;
        }
        self.shares.revoke(&b.identity.instance_id).await;
        self.apply_firewall().await
    }
}
