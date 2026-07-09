use std::io::Result;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto");
    let proto_dir = "proto";
    let proto_files: Vec<PathBuf> = fs::read_dir(proto_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |e| e == "proto"))
        .collect();

    prost_build::compile_protos(&proto_files, &[proto_dir])?;
    Ok(())
    
}