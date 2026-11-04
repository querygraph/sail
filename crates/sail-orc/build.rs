use sail_build_scripts::data_source::options::build_data_source_options;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    build_data_source_options("Orc", "orc")?;
    build_data_source_options("OrcCompression", "orc_write")?;
    build_data_source_options("OrcListing", "orc_listing")?;
    Ok(())
}
