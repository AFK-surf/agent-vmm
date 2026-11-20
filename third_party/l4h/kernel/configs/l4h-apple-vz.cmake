# seL4 kernel configuration for l4h on aarch64 (Apple Virtualization.framework; GICv3).
# EL1 kernel: no hardware virtualization, direct FDT boot via VZLinuxBootLoader.
set(KernelPlatform "apple-vz" CACHE STRING "")
set(KernelSel4Arch "aarch64" CACHE STRING "")
set(KernelArmHypervisorSupport OFF CACHE BOOL "")
set(KernelArmGicV3 ON CACHE BOOL "")
set(KernelIsMCS ON CACHE BOOL "")
set(KernelMaxNumNodes "16" CACHE STRING "")
set(KernelFastpath OFF CACHE BOOL "")
set(KernelVerificationBuild OFF CACHE BOOL "")
set(KernelOptimisation "-O2" CACHE STRING "")
set(KernelAllowSMCCalls ON CACHE BOOL "")
# Guests and host components read the virtual counter (seL4 deadlines).
set(KernelArmExportVCNTUser ON CACHE BOOL "")
set(KernelRootCNodeSizeBits "19" CACHE STRING "")
set(KernelMaxNumBootinfoUntypedCaps "230" CACHE STRING "")
set(KernelRetypeFanOutLimit "16384" CACHE STRING "")
set(KernelTimerTickMS "2" CACHE STRING "")
set(KernelDebugBuild ON CACHE BOOL "")
# Debug syscalls only for host components (see l4h-x86_64.cmake).
set(KernelDebugSyscallMinMCP "101" CACHE STRING "")
set(KernelPrinting ON CACHE BOOL "")
set(KernelIRQReporting OFF CACHE BOOL "")
