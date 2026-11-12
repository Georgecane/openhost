use openhost::capability::{
    Capability, ComputeCapability, Latency, Lifetime, MemoryCapability, NetworkCapability,
    Reliability, Security, StorageCapability,
};
use openhost::discovery::{
    Advertisement, DiscoveryBeacon, DiscoveryListener, FreshnessPolicy, MemoryRegistry,
    Registry as DiscoveryRegistry,
};
use openhost::endpoint::{ParticipantExecutionEndpoint, RegistryRuntimeAdapter};
use openhost::execution::{ExecutionUnit, WorkItem};
use openhost::fabric::{MemoryFabric, ResourceOffer};
use openhost::identity::{Identity, Kind};
use openhost::node::RuntimeSpec;
use openhost::resource::{Cpu, Memory, ResourceFragment};
use openhost::runtime::{EchoRuntime, ProcessCommand, ProcessRuntime, RuntimeRegistry};
use openhost::scheduler::{AggregatingScheduler, Scheduler, SchedulingStrategy};
use openhost::transport::{
    ExecutionEndpoint, ExecutionRequest, ExecutionStatus, TcpEndpointServer, TcpTransport,
    Transport,
};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

fn print_help() {
    println!("OpenHost: The infrastructure is the network, not the machine.\n");
    println!("Usage: openhost <command> [options]\n");
    println!("Commands:");
    println!("  demo                       Run in-memory allocation and scheduling demo (default)");
    println!("  plan [options]             Compute resource allocation plan");
    println!("      --cpu <cores>          Required CPU cores (default: 1.0)");
    println!("      --memory-mb <mb>       Required memory in MB (default: 512)");
    println!("      --strategy <strat>     Strategy: id | min-frag (default: id)");
    println!("  participant [options]      Run participant daemon listening for execution");
    println!("      --bind <addr:port>     TCP bind address (default: 127.0.0.1:7443)");
    println!("      --cpu <cores>          Offered CPU cores (default: 2.0)");
    println!("      --memory-mb <mb>       Offered memory in MB (default: 1024)");
    println!("      --beacon <addr:port>   Optional UDP broadcast address for discovery");
    println!("      --allow-program <path> Explicitly allow a local process runtime command (loopback only)");
    println!("      --process-timeout <s>  Process execution timeout in seconds (default: 10)");
    println!("  discover [options]         Listen for UDP peer discovery beacons");
    println!("      --bind <addr:port>     UDP listen address (default: 0.0.0.0:7444)");
    println!("      --duration <secs>      Optional listening duration in seconds");
    println!(
        "  vm                         Demonstrate leased virtual machine lifecycle & self-healing"
    );
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
    let mut strategy = SchedulingStrategy::DeterministicId;

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
            "--strategy" if i + 1 < args.len() => {
                match args[i + 1].as_str() {
                    "min-frag" | "min-fragmentation" => {
                        strategy = SchedulingStrategy::MinFragmentation;
                    }
                    _ => {
                        strategy = SchedulingStrategy::DeterministicId;
                    }
                }
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

    let scheduler = AggregatingScheduler::with_strategy(fabric, strategy);
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
    let mut allowed_programs = Vec::new();
    let mut process_timeout_secs = 10u64;

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
            "--allow-program" if i + 1 < args.len() => {
                allowed_programs.push(args[i + 1].clone());
                i += 2;
            }
            "--process-timeout" if i + 1 < args.len() => {
                process_timeout_secs = args[i + 1].parse().unwrap_or(10);
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
    if !allowed_programs.is_empty() {
        let loopback_only = bind_addr
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|address| address.ip().is_loopback());

        if loopback_only {
            registry
                .register(
                    RuntimeSpec::new("process", "1"),
                    Arc::new(
                        ProcessRuntime::with_allowlist("process", allowed_programs)
                            .with_timeout(Duration::from_secs(process_timeout_secs.max(1))),
                    ),
                )
                .expect("register allowlisted process runtime");
            println!("Process runtime enabled with an explicit allowlist and timeout.");
        } else {
            eprintln!(
                "Process runtime disabled: this CLI participant uses unauthenticated TCP;                  process execution is only enabled when bound to a loopback address."
            );
        }
    } else {
        println!("Process runtime disabled by default. Use --allow-program on loopback to opt in.");
    }

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
        match ProcessCommand::new(prog).args(prog_args).encode() {
            Ok(payload) => payload,
            Err(error) => {
                eprintln!("Invalid process command: {:?}", error);
                return;
            }
        }
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

fn run_discover(args: &[String]) {
    let mut bind_addr = "0.0.0.0:7444".to_string();
    let mut duration_secs: Option<u64> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--bind" if i + 1 < args.len() => {
                bind_addr = args[i + 1].clone();
                i += 2;
            }
            "--duration" if i + 1 < args.len() => {
                duration_secs = args[i + 1].parse().ok();
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let policy = FreshnessPolicy {
        stale_after: Duration::from_secs(30),
        expire_after: Duration::from_secs(60),
    };
    let registry = Arc::new(MemoryRegistry::new(policy).expect("create discovery registry"));
    let listener = match DiscoveryListener::bind(
        &bind_addr,
        Arc::clone(&registry) as Arc<dyn openhost::discovery::Registry>,
    ) {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "Failed to bind discovery listener on {}: {:?}",
                bind_addr, e
            );
            return;
        }
    };

    println!("OpenHost Discovery Listener bound to {}", bind_addr);
    println!(
        "Listening for peer advertisement beacons (wire protocol v{})...",
        openhost::discovery::DISCOVERY_PROTOCOL_VERSION
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_signal = Arc::clone(&shutdown);

    if let Some(dur) = duration_secs {
        println!("Will listen for {} second(s)...", dur);
        let s = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(dur));
            s.store(true, Ordering::Relaxed);
        });
    } else {
        println!("Press Ctrl+C to terminate.\n");
    }

    let listener_handle = std::thread::spawn(move || {
        let _ = listener.listen_until(&shutdown_signal);
    });

    let start = std::time::Instant::now();
    let mut last_count = 0;
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(200));
        let members = registry.members();
        if members.len() != last_count {
            last_count = members.len();
            println!("\nDiscovered Cluster Participants ({}):", members.len());
            for (idx, m) in members.iter().enumerate() {
                let cap = &m.advertisement.capability;
                println!(
                    "  [{}] Participant: {}\n      Compute: {:.2} CPU cores, {} GPU units | Memory: {} MB | Storage: {} GB\n      Bandwidth: {} Mbps | Sequence: #{}",
                    idx + 1,
                    m.advertisement.participant.id,
                    cap.compute.cpu_cores,
                    cap.compute.gpu_units,
                    cap.memory.bytes >> 20,
                    cap.storage.bytes >> 30,
                    cap.network.bits_per_second / 1_000_000,
                    m.advertisement.sequence
                );
            }
        }
        if let Some(dur) = duration_secs
            && start.elapsed() >= Duration::from_secs(dur)
        {
            shutdown.store(true, Ordering::Relaxed);
            break;
        }
    }

    let _ = listener_handle.join();
    println!("Discovery listener stopped.");
}

fn run_vm() {
    use openhost::control::Plane;
    use openhost::discovery::{FreshnessPolicy, MemoryRegistry as DiscoveryMemoryRegistry};
    use openhost::execution::NoopBackend;
    use openhost::participant::Participant;
    use openhost::registry::Registry;

    println!("OpenHost Control Plane & Leased VM Demonstration");
    println!("=================================================");

    let registry = Arc::new(Registry::new());
    let discovery = Arc::new(
        DiscoveryMemoryRegistry::new(FreshnessPolicy {
            stale_after: Duration::from_secs(30),
            expire_after: Duration::from_secs(60),
        })
        .unwrap(),
    );
    let scheduler = Arc::new(AggregatingScheduler::new(registry.clone()));
    let plane = Plane::new(registry.clone(), discovery, scheduler);

    let now = SystemTime::now();

    // 1. Register 3 participants
    let p1_id = Identity::new(Kind::Participant);
    let p2_id = Identity::new(Kind::Participant);
    let p3_id = Identity::new(Kind::Participant);

    let p1 = Arc::new(
        Participant::new(
            p1_id.clone(),
            ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                memory: Memory { bytes: 512 << 20 },
                ..Default::default()
            }
            .to_capability(0.99, Duration::from_millis(5), true),
            ResourceFragment {
                cpu: Cpu { cores: 1.0 },
                memory: Memory { bytes: 512 << 20 },
                ..Default::default()
            },
            now,
        )
        .unwrap(),
    );
    p1.activate(now).unwrap();
    plane.register_participant(p1.clone()).unwrap();

    let p2 = Arc::new(
        Participant::new(
            p2_id.clone(),
            ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                memory: Memory { bytes: 1024 << 20 },
                ..Default::default()
            }
            .to_capability(0.99, Duration::from_millis(5), true),
            ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                memory: Memory { bytes: 1024 << 20 },
                ..Default::default()
            },
            now,
        )
        .unwrap(),
    );
    p2.activate(now).unwrap();
    plane.register_participant(p2.clone()).unwrap();

    let p3 = Arc::new(
        Participant::new(
            p3_id.clone(),
            ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                memory: Memory { bytes: 1024 << 20 },
                ..Default::default()
            }
            .to_capability(0.99, Duration::from_millis(5), true),
            ResourceFragment {
                cpu: Cpu { cores: 2.0 },
                memory: Memory { bytes: 1024 << 20 },
                ..Default::default()
            },
            now,
        )
        .unwrap(),
    );
    p3.activate(now).unwrap();

    println!("1. Registered initial active participants in fabric:");
    println!(
        "   - Participant 1 ({}): 1.0 CPU cores, 512 MB RAM",
        p1_id.id
    );
    println!(
        "   - Participant 2 ({}): 2.0 CPU cores, 1024 MB RAM",
        p2_id.id
    );

    // 2. Create Leased Virtual Machine
    let req = ResourceFragment {
        cpu: Cpu { cores: 2.5 },
        memory: Memory { bytes: 1280 << 20 },
        ..Default::default()
    };
    println!("\n2. Scheduling and creating leased Virtual Machine...");
    println!("   Required: 2.5 CPU cores, 1280 MB RAM");
    let lease_duration = Duration::from_secs(120);
    let (vm, leases) = plane
        .create_leased_virtual_machine(req, RuntimeSpec::new("echo", "1"), now, lease_duration)
        .expect("create leased VM");

    println!("   Virtual Machine ID: {}", vm.id.id);
    println!("   Logical Node ID:    {}", vm.node_id.id);
    println!("   Backing physical leases ({}):", leases.len());
    for (i, lease) in leases.iter().enumerate() {
        println!(
            "     [{}] Lease {} -> Participant {} ({:.2} cores, {} MB)",
            i + 1,
            lease.id.id,
            lease.participant_id.id,
            lease.resources.cpu.cores,
            lease.resources.memory.bytes >> 20
        );
    }

    // 3. Start VM and Execute Workload
    println!("\n3. Starting Virtual Machine and executing workload...");
    plane.start_virtual_machine(&vm.id).unwrap();
    let workload = WorkItem {
        id: "workload-alpha".into(),
        payload: b"OpenHost distributed VM workload".to_vec(),
    };
    let backend = NoopBackend;
    let receipt = plane
        .execute_virtual_machine(&vm.id, &workload, &backend)
        .expect("execution succeeds");
    println!(
        "   Workload '{}' dispatched across {} execution unit(s) successfully!",
        receipt.workload_id, receipt.dispatched_units
    );

    // 4. Simulate Failure & Audit
    println!("\n4. Simulating failure/drain of Participant 1...");
    p1.drain(now + Duration::from_secs(10)).unwrap();

    let unhealthy = plane
        .audit_virtual_machine_health(&vm.id, now + Duration::from_secs(10))
        .expect("health audit");
    println!("   Audit detected unhealthy participant(s):");
    for u in &unhealthy {
        println!("     - Participant {}", u.id);
    }

    // 5. Dynamic Self-Healing
    println!("\n5. Registering spare Participant 3 and performing dynamic VM healing...");
    plane.register_participant(p3.clone()).unwrap();

    let (healed_vm, new_lease) = plane
        .heal_virtual_machine(
            &vm.id,
            &p1_id,
            now + Duration::from_secs(10),
            Duration::from_secs(110),
        )
        .expect("heal VM");

    println!("   Virtual Machine healed successfully without teardown!");
    println!(
        "   New replacement lease: {} -> Participant {}",
        new_lease.id.id, new_lease.participant_id.id
    );
    println!("   Healed VM State: {:?}", healed_vm.state);

    let audit_after = plane
        .audit_virtual_machine_health(&vm.id, now + Duration::from_secs(10))
        .expect("post-heal audit");
    println!(
        "   Post-healing unhealthy participants count: {}",
        audit_after.len()
    );
    println!("\nAll VM operations completed cleanly!");
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let command = args.first().map(|s| s.as_str()).unwrap_or("demo");

    match command {
        "demo" => run_demo(),
        "plan" => run_plan(&args[1..]),
        "participant" | "daemon" => run_participant(&args[1..]),
        "discover" | "listen" => run_discover(&args[1..]),
        "vm" | "control" => run_vm(),
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

    #[test]
    fn plan_runs_without_panic() {
        run_plan(&[
            "--cpu".into(),
            "1.5".into(),
            "--memory-mb".into(),
            "768".into(),
        ]);
        run_plan(&[
            "--cpu".into(),
            "2.0".into(),
            "--memory-mb".into(),
            "1024".into(),
            "--strategy".into(),
            "min-frag".into(),
        ]);
    }

    #[test]
    fn vm_demo_runs_without_panic() {
        run_vm();
    }
}
