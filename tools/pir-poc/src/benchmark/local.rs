use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::dense;
use crate::snapshot::Snapshot;

pub(super) struct Evaluation {
    pub answers: Vec<Vec<Vec<u8>>>,
    pub wall: Duration,
    pub sum_server_elapsed: Duration,
}

struct Job {
    query_shares: Vec<Vec<u8>>,
    response: mpsc::Sender<Response>,
}

struct Response {
    server_index: usize,
    answers: std::result::Result<Vec<Vec<u8>>, String>,
    elapsed: Duration,
}

pub(super) struct LocalServerPool {
    senders: Vec<mpsc::Sender<Job>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl LocalServerPool {
    pub fn new(
        snapshot: Arc<Snapshot>,
        server_count: usize,
        worker_threads: usize,
    ) -> Result<Self> {
        let mut senders = Vec::with_capacity(server_count);
        let mut workers = Vec::with_capacity(server_count);
        for server_index in 0..server_count {
            let (sender, receiver) = mpsc::channel::<Job>();
            let snapshot = Arc::clone(&snapshot);
            let evaluator = dense::ParallelEvaluator::new(worker_threads)?;
            workers.push(std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let started = Instant::now();
                    let answers = evaluator
                        .answer_batch(snapshot.view(), &job.query_shares)
                        .map_err(|error| error.to_string());
                    let _ = job.response.send(Response {
                        server_index,
                        answers,
                        elapsed: started.elapsed(),
                    });
                }
            }));
            senders.push(sender);
        }
        Ok(Self { senders, workers })
    }

    pub fn evaluate(&self, per_server_queries: Vec<Vec<Vec<u8>>>) -> Result<Evaluation> {
        if per_server_queries.len() != self.senders.len() {
            bail!("server and query share counts differ");
        }
        let (response_sender, response_receiver) = mpsc::channel();
        let wall_started = Instant::now();
        for (server, query_shares) in self.senders.iter().zip(per_server_queries) {
            server
                .send(Job {
                    query_shares,
                    response: response_sender.clone(),
                })
                .context("send benchmark query to server worker")?;
        }
        drop(response_sender);

        let mut answers = (0..self.senders.len()).map(|_| None).collect::<Vec<_>>();
        let mut sum_server_elapsed = Duration::ZERO;
        for _ in 0..self.senders.len() {
            let response = response_receiver
                .recv()
                .context("receive benchmark server answer")?;
            sum_server_elapsed += response.elapsed;
            answers[response.server_index] = Some(response.answers.map_err(anyhow::Error::msg)?);
        }
        Ok(Evaluation {
            answers: answers
                .into_iter()
                .map(|answer| answer.context("benchmark server returned no answer"))
                .collect::<Result<Vec<_>>>()?,
            wall: wall_started.elapsed(),
            sum_server_elapsed,
        })
    }
}

impl Drop for LocalServerPool {
    fn drop(&mut self) {
        self.senders.clear();
        for worker in self.workers.drain(..) {
            worker.join().expect("benchmark server worker panicked");
        }
    }
}
