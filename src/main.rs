use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::*;
use differential_dataflow::AsCollection;

use std::fs::File;
use std::io::{BufRead, BufReader};
// NEW: BufWriter + Write for logging to files
use std::io::{BufWriter, Write};

use std::time::Instant;

// NEW: imports for logging
use std::time::Duration;
use timely::container::CapacityContainerBuilder;
use timely::logging::{StartStop, TimelyEvent, TimelyProgressEventBuilder};
use timely::logging::{TimelyEventBuilder, TimelySummaryEventBuilder};
use timely::progress::reachability::logging::TrackerEventBuilder;

use timely::dataflow::operators::core::*;
use timely::dataflow::Scope;

// NEW: minimal imports for aggregation
use std::collections::HashMap;
// NEW: fix move into closure
use std::cell::RefCell;
use std::rc::Rc;

type Diff = differential_dataflow::difference::Present;
type Iter = u16;
const SEMIRING_ONE: Diff = differential_dataflow::difference::Present;

// NEW: minimal stats struct
#[derive(Clone, Debug, Default)]
struct OpStats {
    name: String,
    addr: String,
    total_active: Duration,
    activations: u64,
    current_start: Option<Duration>,
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let timer = Instant::now();
        let peers = worker.peers();
        let index = worker.index();

        // =========================================================
        // NEW BLOCK: register Timely logging on this worker,
        // writing each stream to its own file.
        // =========================================================

        // NEW: per-worker aggregate map (minimal; shared with the closure)
        let op_stats: Rc<RefCell<HashMap<usize, OpStats>>> = Rc::new(RefCell::new(HashMap::new()));
        let op_stats_in_log = op_stats.clone();

        // 1) Core Timely events: operator creation, scheduling, channels, etc.
        // let timely_file = File::create(format!("timely_worker_{}.log", index))
        //     .expect("failed to create timely log file");
        // let mut timely_writer = BufWriter::new(timely_file);

        // worker
        //     .log_register()
        //     .unwrap()
        //     .insert::<TimelyEventBuilder, _>("timely", move |time, data| {
        //         if let Some(data) = data {
        //             for event in data.iter() {
        //                 // you can adjust the format later as needed
        //                 writeln!(timely_writer, "[time {:?}] {:?}", time, event)
        //                     .expect("failed to write timely log");
        //             }
        //         } else {
        //             writeln!(timely_writer, "[flush at {:?}]", time)
        //                 .expect("failed to write timely flush");
        //             timely_writer.flush().expect("failed to flush timely log");
        //         }
        //     });

        let op_file = File::create(format!("operators_worker_{}.log", index))
            .expect("failed to create operator log file");
        let mut op_writer = BufWriter::new(op_file);

        worker
            .log_register()
            .unwrap()
            .insert::<TimelyEventBuilder, _>("timely", move |_batch_time, data| {
                if let Some(data) = data {
                    for (ts, event) in data.iter() {
                        match event {
                            // Operator created: id, addr, name
                            TimelyEvent::Operates(op) => {
                                writeln!(
                                    op_writer,
                                    "[{:?}] OPERATES id={} addr={:?} name={}",
                                    ts, op.id, op.addr, op.name
                                )
                                .expect("failed to write OPERATES log");

                                // NEW: record metadata in hashmap
                                let mut map = op_stats_in_log.borrow_mut();
                                let entry = map.entry(op.id).or_default();
                                entry.name = op.name.to_string();
                                entry.addr = format!("{:?}", op.addr);
                            }
                            // Operator scheduled: start/stop with timestamp
                            TimelyEvent::Schedule(sched) => {
                                // NEW: accumulate (Stop - Start) and activation count.
                                let mut map = op_stats_in_log.borrow_mut();
                                let entry = map.entry(sched.id).or_default();
                                match sched.start_stop {
                                    StartStop::Start => {
                                        entry.current_start = Some(*ts);
                                    }
                                    StartStop::Stop => {
                                        if let Some(st) = entry.current_start.take() {
                                            let delta =
                                                ts.checked_sub(st).unwrap_or(Duration::ZERO);
                                            entry.total_active += delta;
                                            entry.activations += 1;
                                        }
                                    }
                                }

                                // keep reference code in place; don't spam logs
                                // let kind = match sched.start_stop {
                                //     StartStop::Start => "START",
                                //     StartStop::Stop => "STOP",
                                // };
                                // writeln!(op_writer, "[{:?}] SCHEDULE {} id={}", ts, kind, sched.id)
                                //     .expect("failed to write SCHEDULE log");
                            }
                            // ignore everything else
                            _ => {}
                        }
                    }
                } else {
                    // flush marker; flush file too
                    op_writer.flush().expect("failed to flush operator log");
                }
            });

        // 2) Timely progress logging (optional).
        // let progress_file = File::create(format!("progress_worker_{}.log", index))
        //     .expect("failed to create progress log file");
        // let mut progress_writer = BufWriter::new(progress_file);

        // worker
        //     .log_register()
        //     .unwrap()
        //     .insert::<TimelyProgressEventBuilder<usize>, _>(
        //         "timely/progress/usize",
        //         move |time, data| {
        //             if let Some(data) = data {
        //                 for ev in data.iter() {
        //                     writeln!(progress_writer, "[time {:?}] {:?}", time, ev)
        //                         .expect("failed to write progress log");
        //                 }
        //             } else {
        //                 writeln!(progress_writer, "[flush at {:?}]", time)
        //                     .expect("failed to write progress flush");
        //                 progress_writer
        //                     .flush()
        //                     .expect("failed to flush progress log");
        //             }
        //         },
        //     );

        // 3) Reachability logging (optional).
        // let reach_file = File::create(format!("reachability_worker_{}.log", index))
        //     .expect("failed to create reachability log file");
        // let mut reach_writer = BufWriter::new(reach_file);

        // worker
        //     .log_register()
        //     .unwrap()
        //     .insert::<TrackerEventBuilder<usize>, _>(
        //         "timely/reachability/usize",
        //         move |time, data| {
        //             if let Some(data) = data {
        //                 for ev in data.iter() {
        //                     writeln!(reach_writer, "[time {:?}] {:?}", time, ev)
        //                         .expect("failed to write reachability log");
        //                 }
        //             } else {
        //                 writeln!(reach_writer, "[flush at {:?}]", time)
        //                     .expect("failed to write reachability flush");
        //                 reach_writer
        //                     .flush()
        //                     .expect("failed to flush reachability log");
        //             }
        //         },
        //     );

        // 4) Summary events: per-operator aggregated statistics.
        // let summary_file = File::create(format!("summary_worker_{}.log", index))
        //     .expect("failed to create summary log file");
        // let mut summary_writer = BufWriter::new(summary_file);

        // worker
        //     .log_register()
        //     .unwrap()
        //     // NOTE: use `()` here to match your dataflow timestamp type.
        //     .insert::<TimelySummaryEventBuilder<usize>, _>(
        //         "timely/summary/usize",
        //         move |_time, data| {
        //             if let Some(data) = data {
        //                 // `data` is a batch of (ts, summary_event) pairs
        //                 for (_ts, summary) in data.iter() {
        //                     // Start simple: just dump the Debug. You’ll see fields like
        //                     // id, activations, total time, maybe histograms depending on timely version.
        //                     writeln!(summary_writer, "{:?}", summary)
        //                         .expect("failed to write summary log");
        //                 }
        //             } else {
        //                 // just flush, no extra "[flush ...]" spam
        //                 summary_writer.flush().expect("failed to flush summary log");
        //             }
        //         },
        //     );

        // 5) Example user-level log stream (optional).
        // type MyBuilder = CapacityContainerBuilder<Vec<(Duration, ())>>;
        // let input_file = File::create(format!("input_worker_{}.log", index))
        //     .expect("failed to create input log file");
        // let mut input_writer = BufWriter::new(input_file);

        // worker
        //     .log_register()
        //     .unwrap()
        //     .insert::<MyBuilder, _>("input", move |time, data| {
        //         if let Some(data) = data {
        //             for (ts, _) in data.iter() {
        //                 writeln!(input_writer, "tick at {:?}", ts)
        //                     .expect("failed to write input log");
        //             }
        //         } else {
        //             writeln!(input_writer, "[flush at {:?}]", time)
        //                 .expect("failed to write input flush");
        //             input_writer.flush().expect("failed to flush input log");
        //         }
        //     });

        // // Handle for user-level logger.
        // let input_logger = worker
        //     .log_register()
        //     .unwrap()
        //     .get::<MyBuilder>("input")
        //     .expect("input logger absent");

        // =========================================================
        // END of NEW logging setup
        // =========================================================

        let mut harc = worker.dataflow::<(), _, _>(|scope| {
            let (harc, arc) = scope.new_collection::<_, Diff>();

            let t_10340945921525095033 =
                arc.flat_map(|(x0, x1): (i32, i32)| std::iter::once((x0, x1)));

            let tc = t_10340945921525095033
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE));

            let t_12902220955919095999 =
                arc.flat_map(|(x0, x1): (i32, i32)| std::iter::once(((x0,), (x1,))));

            let t_12902220955919095999_arr = t_12902220955919095999.arrange_by_key();

            let tc = scope.iterative::<Iter, _, _>(|inner| {
                let in_tc = tc.enter(inner);
                let in_t_12902220955919095999_arr = t_12902220955919095999_arr.enter(inner);

                let iter_13606748612899232454 = SemigroupVariable::new(
                    inner,
                    timely::order::Product::new(Default::default(), 1),
                );

                let t_15271755969968327840 = iter_13606748612899232454
                    .flat_map(|(x0, x1): (i32, i32)| std::iter::once(((x1,), (x0,))));

                let t_15271755969968327840_arr = t_15271755969968327840.arrange_by_key();

                let t_8146662962375053354 = t_15271755969968327840_arr
                    .join_core(&in_t_12902220955919095999_arr, |_k, lv, rv| {
                        Some((lv.0, rv.0))
                    });

                let next_13606748612899232454 = t_8146662962375053354
                    .concat(&in_tc)
                    .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE));

                iter_13606748612899232454.set(&next_13606748612899232454);
                next_13606748612899232454.leave()
            });

            {
                tc.threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
                    .inner
                    .flat_map(move |(_, t, _)| std::iter::once(((), t.clone(), 1 as i32)))
                    .as_collection()
                    .map(|_| ())
                    .consolidate()
                    .inspect(|(_data, _time, size)| eprintln!("[size] [{}] {:?}", "tc", size));
            }
            harc
        });

        if index == 0 {
            println!("{:?}:\tDataflow assembled", timer.elapsed());
        }

        let ingest = {
            let reader = BufReader::new(
                File::open("Arc.csv")
                    .unwrap_or_else(|e| panic!("failed to open {}: {}", "Arc.csv", e)),
            );
            reader
                .split(b'\n')
                .filter_map(Result::ok)
                .filter(move |line| !line.is_empty())
                .filter_map(move |line| {
                    let mut tuple = line.split(|&bt| bt == ",".as_bytes()[0]);

                    let f0: i32 = std::str::from_utf8(tuple.next()?)
                        .ok()?
                        .parse::<i32>()
                        .ok()?;
                    let should_send = ((f0 as usize) % peers) == index;
                    if !should_send {
                        return None;
                    }
                    let f1: i32 = std::str::from_utf8(tuple.next()?)
                        .ok()?
                        .parse::<i32>()
                        .ok()?;
                    Some((f0, f1))
                })
        };

        ingest.for_each(|row| harc.update(row, SEMIRING_ONE));
        harc.close();

        // Optional: mark a "round" tick in the user input log
        // input_logger.log(());

        if index == 0 {
            println!("{:?}:\tData loaded for {}", timer.elapsed(), "arc");
        }

        while worker.step() {}

        // NEW: write final per-operator totals (minimal; separate file)
        {
            let map = op_stats.borrow();
            let mut rows: Vec<(usize, OpStats)> =
                map.iter().map(|(id, st)| (*id, st.clone())).collect();
            rows.sort_by_key(|(id, _st)| *id);

            let stats_file = File::create(format!("operator_stats_worker_{}.log", index))
                .expect("failed to create operator stats log file");
            let mut stats_writer = BufWriter::new(stats_file);

            // header (left-aligned)
            writeln!(
                stats_writer,
                "{:<20} {:<12} {:<16} {}",
                "addr", "activations", "total_active_ms", "name"
            )
            .ok();

            for (_id, st) in rows {
                let total_ms = st.total_active.as_secs_f64() * 1000.0;

                writeln!(
                    stats_writer,
                    "{:<20} {:<12} {:<16.3} {}",
                    st.addr, st.activations, total_ms, st.name
                )
                .ok();
            }

            stats_writer.flush().ok();
        }

        if index == 0 {
            println!("{:?}:\tDataflow executed", timer.elapsed());
        }
    })
    .unwrap();
}
