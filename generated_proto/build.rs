fn main() {
    let base = std::env::current_dir().unwrap();
    let parent =  base.parent().unwrap();
    let example_proto_file = parent.join("proto").join("example.proto").as_path().to_str().unwrap().to_string(); 
    let sb_proto_file = parent.join("proto").join("service_bus.proto").as_path().to_str().unwrap().to_string(); 

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("./src")
        .compile(&[&example_proto_file, &sb_proto_file], &[parent])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    println!("cargo:rerun-if-changed={}", &example_proto_file);
    println!("cargo:rerun-if-changed={}", &sb_proto_file);
}
