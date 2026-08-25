----------------------- MODULE SystemInventorySeparation ----------------------
EXTENDS FiniteSets

CONSTANT EnforceSystemSeparation

VARIABLE tenantInventory

vars == <<tenantInventory>>

Init == tenantInventory = {"tenant"}

RefreshTenantInventory == UNCHANGED tenantInventory

ExposeSystem ==
  /\ ~EnforceSystemSeparation
  /\ tenantInventory' = tenantInventory \cup {"system"}

Next == RefreshTenantInventory \/ ExposeSystem

TypeOK == tenantInventory \subseteq {"tenant", "system"}
SystemExcludedFromInventory == "system" \notin tenantInventory

Spec == Init /\ [][Next]_vars

=============================================================================
