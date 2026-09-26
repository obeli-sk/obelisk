pub const SCHEDULE_SUFFIX: &str = "-obelisk-schedule";
pub const EXT_SUFFIX: &str = "-obelisk-ext";
pub const STUB_SUFFIX: &str = "-obelisk-stub";

#[must_use]
pub fn strip_specifier_suffix(specifier: &str, suffix: &str) -> Option<String> {
    let slash_pos = specifier.find('/')?;
    let pkg_part = &specifier[..slash_pos];
    let ifc_part = &specifier[slash_pos..];
    pkg_part
        .strip_suffix(suffix)
        .map(|base_pkg| format!("{base_pkg}{ifc_part}"))
}
