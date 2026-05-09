use redis::Script;

pub(crate) mod guard {
    pub(crate) const ACQUIRE_OCCURRENCE: &str =
        include_str!("lua/valkey_guard/acquire_occurrence.lua");
    pub(crate) const ACQUIRE_RESOURCE: &str = include_str!("lua/valkey_guard/acquire_resource.lua");
    pub(crate) const RENEW_OCCURRENCE: &str = include_str!("lua/valkey_guard/renew_occurrence.lua");
    pub(crate) const RENEW_RESOURCE: &str = include_str!("lua/valkey_guard/renew_resource.lua");
    pub(crate) const RELEASE_OCCURRENCE: &str =
        include_str!("lua/valkey_guard/release_occurrence.lua");
    pub(crate) const RELEASE_RESOURCE: &str = include_str!("lua/valkey_guard/release_resource.lua");
}

pub(crate) mod coordinated {
    pub(crate) const SAVE_STATE: &str = include_str!("lua/valkey_coordinated_store/save_state.lua");
    pub(crate) const RECLAIM_INFLIGHT: &str =
        include_str!("lua/valkey_coordinated_store/reclaim_inflight.lua");
    pub(crate) const CLAIM_TRIGGER: &str =
        include_str!("lua/valkey_coordinated_store/claim_trigger.lua");
    pub(crate) const RENEW_LEASE: &str =
        include_str!("lua/valkey_coordinated_store/renew_lease.lua");
    pub(crate) const COMPLETE: &str = include_str!("lua/valkey_coordinated_store/complete.lua");
    pub(crate) const PAUSE: &str = include_str!("lua/valkey_coordinated_store/pause.lua");
    pub(crate) const RESUME: &str = include_str!("lua/valkey_coordinated_store/resume.lua");
}

pub(crate) fn script(source: &'static str) -> Script {
    Script::new(source)
}
