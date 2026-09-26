use openhost::fabric::{MemoryFabric, ResourceOffer};
use openhost::resource::{Cpu, Memory, ResourceFragment};
use openhost::scheduler::{AggregatingScheduler, Scheduler};
use std::sync::Arc;

fn main() {
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
