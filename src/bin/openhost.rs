use openhost::capability::{
    Capability, ComputeCapability, Latency, Lifetime, MemoryCapability, NetworkCapability,
    Reliability, Security, StorageCapability,
};
use openhost::discovery::{Advertisement, DiscoveryBeacon};
use openhost::endpoint::{ParticipantExecutionEndpoint, RegistryRuntimeAdapter};
use openhost::execution::{ExecutionUnit, WorkItem};
use openhost::fabric::{MemoryFabric, ResourceOffer};
use openhost::identity::{Identity, Kind};
use openhost::node::RuntimeSpec;
use openhost::resource::{Cpu, Memory, ResourceFragment};
use openhost::runtime::{EchoRuntime, ProcessCommand, ProcessRuntime, RuntimeRegistry};
use openhost::scheduler::{AggregatingScheduler, Scheduler};
use openhost::transport::{
    ExecutionEndpoint, ExecutionRequest, ExecutionStatus, TcpEndpointServer, TcpTransport,
    Transport,
};
use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime};

fn print_help() {
    println!("OpenHost: The infrastructure is the network, not the machine.\n");
    println!("Usage: openhost <command> [options]\n");
    println!("Commands:");
    println!("  demo                       Run in-memory allocation and scheduling demo (default)");
    println!("  plan [options]             Compute resource allocation plan");
    println!("      --cpu <cores>          Required CPU cores (default: 1.0)");
    println!("      --memory-mb <mb>       Required memory in MB (default: 512)");
    println!("  participant [options]      Run participant daemon listening for execution");
    println!("      --bind <addr:port>     TCP bind address (default: 127.0.0.1:7443)");
    println!("      --cpu <cores>          Offered CPU cores (default: 2.0)");
    println!("      --memory-mb <mb>       Offered memory in MB (default: 1024)");
    println!("      --beacon <addr:port>   Optional UDP broadcast address for discovery");
    println!("  exec [options]             Execute a workload on an endpoint");
    println!(
        "      --endpoint <addr:port> Target participant TCP endpoint (default: 127.0.0.1:7443)"
    );
    println!("      --participant <uuid>   Target participant UUID (or generated if omitted)");
    println!("      --runtime <name>       Runtime name (echo | process, default: echo)");
    println!("      --program <prog>       Command to run under process runtime");
    println!("      --args <arg1,arg2>     Comma-separated arguments");
    println!("  version                    Print OpenHost version");
    println!("  help                       Show this help message");
}

fn run_demo() {
    let fabric = Arc::new(MemoryFabric::new());
    fabric
        .upsert(ResourceOffer {
            participant_id: "local-demo".into(),
            resources: ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                memory: Memory { bytes: 256 << 20 },
                ..Default::default()
            },
        })
        .expect("valid demo resource");

    let scheduler = AggregatingScheduler::new(fabric);
    let node = scheduler
        .plan(ResourceFragment {
            cpu: Cpu { cores: 0.5 },
            memory: Memory { bytes: 128 << 20 },
            ..Default::default()
        })
        .expect("demo allocation must succeed");

    println!("OpenHost");
    println!("The infrastructure is the network, not the machine.");
    println!(
        "logical node: {} ({} allocation(s), {:.2} CPU cores, {} bytes memory)",
        node.id,
        node.resources.allocations.len(),
        node.resources.capacity.cpu.cores,
        node.resources.capacity.memory.bytes
    );
}

fn run_plan(args: &[String]) {
    let mut cpu_cores = 1.0;
    let mut memory_mb = 512u64;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--cpu" if i + 1 < args.len() => {
                cpu_cores = args[i + 1].parse().unwrap_or(1.0);
                i += 2;
            }
            "--memory-mb" if i + 1 < args.len() => {
                memory_mb = args[i + 1].parse().unwrap_or(512);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let fabric = Arc::new(MemoryFabric::new());
    // Simulate active fabric contributors
    let p1 = Identity::new(Kind::Participant);
    let p2 = Identity::new(Kind::Participant);
    let p3 = Identity::new(Kind::Participant);

    fabric
        .upsert(ResourceOffer {
            participant_id: p1.id.clone(),
            resources: ResourceFragment {
                cpu: Cpu { cores: 0.5 },
                memory: Memory { bytes: 256 << 20 },
                ..Default::default()
            },
        })
        .unwrap();

    fabric
        .upsert(ResourceOffer {
            participant_id: p2.id.clone(),
            resources: ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                memory: Memory { bytes: 512 << 20 },
                ..Default::default()
            },
        })
        .unwrap();

    fabric
        .upsert(ResourceOffer {
            participant_id: p3.id.clone(),
            resources: ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                memory: Memory { bytes: 1024 << 20 },
                ..Default::default()
            },
        })
        .unwrap();

    let scheduler = AggregatingScheduler::new(fabric);
    let requirement = ResourceFragment {
        cpu: Cpu { cores: cpu_cores },
        memory: Memory {
            bytes: memory_mb << 20,
        },
        ..Default::default()
    };

    match scheduler.plan(requirement) {
        Ok(node) => {
            println!("Plan computed successfully!");
            println!("Logical Node ID: {}", node.id);
            println!(
                "Total Capacity: {:.2} CPU cores, {} MB memory",
                node.resources.capacity.cpu.cores,
                node.resources.capacity.memory.bytes >> 20
            );
            println!(
                "Physical allocations ({}):",
                node.resources.allocations.len()
            );
            for (idx, alloc) in node.resources.allocations.iter().enumerate() {
                println!(
                    "  [{}] Participant {} -> {:.2} cores, {} MB",
                    idx + 1,
                    alloc.participant_id,
                    alloc.resources.cpu.cores,
                    alloc.resources.memory.bytes >> 20
                );
            }
        }
        Err(e) => {
            eprintln!("Failed to compute plan: {:?}", e);
        }
    }
}

fn run_participant(args: &[String]) {
    let mut bind_addr = "127.0.0.1:7443".to_string();
    let mut cpu_cores = 2.0;
    let mut memory_mb = 1024u64;
    let mut beacon_target: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" if i + 1 < args.len() => {
                bind_addr = args[i + 1].clone();
                i += 2;
            }
            "--cpu" if i + 1 < args.len() => {
                cpu_cores = args[i + 1].parse().unwrap_or(2.0);
                i += 2;
            }
            "--memory-mb" if i + 1 < args.len() => {
                memory_mb = args[i + 1].parse().unwrap_or(1024);
                i += 2;
            }
            "--beacon" if i + 1 < args.len() => {
                beacon_target = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let participant_id = Identity::new(Kind::Participant);
    println!("Starting OpenHost Participant: {}", participant_id.id);
    println!(
        "Capacity: {:.2} CPU cores, {} MB memory",
        cpu_cores, memory_mb
    );

    let mut registry = RuntimeRegistry::default();
    registry
        .register(
            RuntimeSpec::new("echo", "1"),
            Arc::new(EchoRuntime::new("echo")),
        )
        .expect("register echo runtime");
    registry
        .register(
            RuntimeSpec::new("process", "1"),
            Arc::new(ProcessRuntime::new("process")),
        )
        .expect("register process runtime");

    let adapter = RegistryRuntimeAdapter::new(Arc::new(registry));
    let endpoint = Arc::new(
        ParticipantExecutionEndpoint::new(participant_id.clone(), adapter)
            .expect("create execution endpoint"),
    );

    let server = TcpEndpointServer::bind(&bind_addr, endpoint).expect("bind tcp server");
    println!("TCP endpoint listening on {}", bind_addr);

    let shutdown = Arc::new(AtomicBool::new(false));

    // Optional discovery beacon
    if let Some(target) = beacon_target {
        let capability = Capability {
            compute: ComputeCapability {
                cpu_cores,
                gpu_units: 0,
            },
            memory: MemoryCapability {
                bytes: memory_mb << 20,
            },
            storage: StorageCapability {
                bytes: 10 * 1024 * 1024 * 1024,
            },
            network: NetworkCapability {
                bits_per_second: 1_000_000_000,
            },
            reliability: Reliability { availability: 0.99 },
            latency: Latency {
                to_participant: Duration::from_millis(10),
            },
            lifetime: Lifetime { duration: None },
            security: Security { trusted: false },
        };
        let ad = Advertisement {
            participant: participant_id,
            capability,
            sequence: 1,
            observed_at: SystemTime::now(),
        };

        if let Ok(beacon) = DiscoveryBeacon::bind("0.0.0.0:0", target.clone(), ad) {
            let shutdown_clone = Arc::clone(&shutdown);
            std::thread::spawn(move || {
                println!("Discovery beacon broadcasting to {}", target);
                let _ = beacon.broadcast_until(&shutdown_clone, Duration::from_secs(3));
            });
        }
    }

    println!("Participant daemon running. Press Ctrl+C to terminate.");
    if let Err(e) = server.serve_until(&shutdown) {
        eprintln!("Server exited with error: {:?}", e);
    }
}

fn run_exec(args: &[String]) {
    let mut endpoint_addr = "127.0.0.1:7443".to_string();
    let mut participant_id_str: Option<String> = None;
    let mut runtime_name = "echo".to_string();
    let mut runtime_ver = "1".to_string();
    let mut program: Option<String> = None;
    let mut prog_args: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint" if i + 1 < args.len() => {
                endpoint_addr = args[i + 1].clone();
                i += 2;
            }
            "--participant" if i + 1 < args.len() => {
                participant_id_str = Some(args[i + 1].clone());
                i += 2;
            }
            "--runtime" if i + 1 < args.len() => {
                runtime_name = args[i + 1].clone();
                i += 2;
            }
            "--version" if i + 1 < args.len() => {
                runtime_ver = args[i + 1].clone();
                i += 2;
            }
            "--program" if i + 1 < args.len() => {
                program = Some(args[i + 1].clone());
                i += 2;
            }
            "--args" if i + 1 < args.len() => {
                prog_args = args[i + 1].split(',').map(|s| s.to_string()).collect();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let participant_id = match participant_id_str {
        Some(s) => match Identity::parse(&s, Kind::Participant) {
            Ok(id) => id,
            Err(_) => {
                eprintln!("Invalid participant identity UUID: {}", s);
                return;
            }
        },
        None => Identity::new(Kind::Participant),
    };

    let payload = if runtime_name == "process" {
        let prog = program.unwrap_or_else(|| "echo".to_string());
        ProcessCommand::new(prog).args(prog_args).encode()
    } else {
        b"OpenHost Ping".to_vec()
    };

    let workload = WorkItem {
        id: format!("work-{}", Identity::new(Kind::VirtualMachine).id),
        payload,
    };

    let node_id = Identity::new(Kind::LogicalNode);
    let unit = ExecutionUnit {
        participant_id: participant_id.clone(),
        resources: ResourceFragment {
            cpu: Cpu { cores: 0.5 },
            memory: Memory { bytes: 128 << 20 },
            ..Default::default()
        },
    };

    let endpoint = match ExecutionEndpoint::new(participant_id.clone(), endpoint_addr.clone()) {
        Ok(ep) => ep,
        Err(e) => {
            eprintln!("Failed to construct endpoint: {:?}", e);
            return;
        }
    };

    let request = ExecutionRequest {
        request_id: format!("{}:{}", workload.id, participant_id.id),
        source_participant_id: None,
        workload,
        node_id,
        runtime: RuntimeSpec::new(runtime_name, runtime_ver),
        unit,
        endpoint,
    };

    println!("Dispatching execution request to {}", endpoint_addr);
    let transport = TcpTransport::new();
    match transport.dispatch(request) {
        Ok(response) => {
            println!("Response status: {:?}", response.status);
            if response.status == ExecutionStatus::Completed {
                println!("Workload completed successfully!");
            } else if let Some(err) = response.error {
                eprintln!("Workload error: {}", err);
            }
        }
        Err(e) => {
            eprintln!("Dispatch error: {:?}", e);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let command = args.first().map(|s| s.as_str()).unwrap_or("demo");

    match command {
        "demo" => run_demo(),
        "plan" => run_plan(&args[1..]),
        "participant" | "daemon" => run_participant(&args[1..]),
        "exec" => run_exec(&args[1..]),
        "version" | "--version" | "-v" => {
            println!("OpenHost 0.1.0 (Rust 2024, wire protocol v1)");
        }
        "help" | "--help" | "-h" => {
            print_help();
        }
        other => {
            eprintln!("Unknown command: {}", other);
            print_help();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_runs_without_panic() {
        run_demo();
    }
}
