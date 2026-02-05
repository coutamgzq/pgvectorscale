use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::iter::{IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator};

const DELTA: f32 = 1e-6;

pub struct LloydKMeans {
    dims: usize,
    c: usize,
    is_spherical: bool,
    centroids: Vec<Vec<f32>>,
    assign: Vec<usize>,
    rng: StdRng,
    samples: Vec<Vec<f32>>,
}

impl LloydKMeans {
    pub fn new(c: usize, samples: Vec<Vec<f32>>, is_spherical: bool, prefer_kmeanspp: bool) -> Self {
        let n = samples.len();
        let dims = if n > 0 { samples[0].len() } else { 0 };

        let mut rng = StdRng::from_entropy();
        let mut centroids = Vec::with_capacity(c);

        if prefer_kmeanspp {
            centroids.push(samples[rng.gen_range(0..n)].clone());
            let mut weight = vec![f32::INFINITY; n];
            for i in 1..c {
                let dis_2 = (0..n)
                    .into_par_iter()
                    .map(|j| squared_distance(&samples[j], &centroids[i - 1]))
                    .collect::<Vec<_>>();
                for j in 0..n {
                    if dis_2[j] < weight[j] {
                        weight[j] = dis_2[j];
                    }
                }
                let sum: f32 = weight.iter().sum();
                let index = 'a: {
                    let mut choice = sum * rng.gen_range(0.0..1.0);
                    for j in 0..(n - 1) {
                        choice -= weight[j];
                        if choice < 0.0f32 {
                            break 'a j;
                        }
                    }
                    n - 1
                };
                centroids.push(samples[index].clone());
            }
        } else {
            let indices: Vec<usize> = rand::seq::index::sample(&mut rng, n, c).into_iter().collect();
            for index in indices {
                centroids.push(samples[index].clone());
            }
        }

        let assign = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (f32::INFINITY, 0);
                for j in 0..c {
                    let dis_2 = squared_distance(&samples[i], &centroids[j]);
                    if dis_2 <= result.0 {
                        result = (dis_2, j);
                    }
                }
                result.1
            })
            .collect::<Vec<_>>();

        Self {
            dims,
            c,
            is_spherical,
            centroids,
            assign,
            rng,
            samples,
        }
    }

    pub fn iterate(&mut self) -> bool {
        let dims = self.dims;
        let c = self.c;
        let rng = &mut self.rng;
        let samples = &self.samples;
        let n = samples.len();

        let (sum, mut count) = (0..n)
            .into_par_iter()
            .fold(
                || (vec![vec![0.0f32; dims]; c], vec![0.0f32; c]),
                |(mut sum, mut count), i| {
                    vector_add_inplace(&mut sum[self.assign[i]], &samples[i]);
                    count[self.assign[i]] += 1.0;
                    (sum, count)
                },
            )
            .reduce(
                || (vec![vec![0.0f32; dims]; c], vec![0.0f32; c]),
                |(mut sum, mut count), (sum_1, count_1)| {
                    for i in 0..c {
                        vector_add_inplace(&mut sum[i], &sum_1[i]);
                        count[i] += count_1[i];
                    }
                    (sum, count)
                },
            );

        let mut centroids = (0..c)
            .into_par_iter()
            .map(|i| vector_mul_scalar(&sum[i], 1.0 / count[i]))
            .collect::<Vec<_>>();

        for i in 0..c {
            if count[i] != 0.0f32 {
                continue;
            }
            let mut o = 0;
            loop {
                let alpha = rng.gen_range(0.0..1.0f32);
                let beta = (count[o] - 1.0) / (n - c) as f32;
                if alpha < beta {
                    break;
                }
                o = (o + 1) % c;
            }
            centroids[i] = centroids[o].clone();
            kmeans_helper(&mut centroids[i], 1.0 + DELTA, 1.0 - DELTA);
            kmeans_helper(&mut centroids[o], 1.0 - DELTA, 1.0 + DELTA);
            count[i] = count[o] / 2.0;
            count[o] -= count[i];
        }

        if self.is_spherical {
            centroids.par_iter_mut().for_each(|centroid| {
                let l = vector_norm(centroid).sqrt();
                if l > 0.0 {
                    vector_mul_scalar_inplace(centroid, 1.0 / l);
                }
            });
        }

        let assign = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (f32::INFINITY, 0);
                for j in 0..c {
                    let dis_2 = squared_distance(&samples[i], &centroids[j]);
                    if dis_2 <= result.0 {
                        result = (dis_2, j);
                    }
                }
                result.1
            })
            .collect::<Vec<_>>();

        let result = (0..n).all(|i| assign[i] == self.assign[i]);

        self.centroids = centroids;
        self.assign = assign;

        result
    }

    pub fn finish(self) -> Vec<Vec<f32>> {
        self.centroids
    }
}

fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum()
}

fn vector_norm(a: &[f32]) -> f32 {
    a.iter().map(|x| x * x).sum()
}

fn vector_add_inplace(a: &mut [f32], b: &[f32]) {
    a.iter_mut().zip(b.iter()).for_each(|(x, y)| *x += y);
}

fn vector_mul_scalar(a: &[f32], scalar: f32) -> Vec<f32> {
    a.iter().map(|x| x * scalar).collect()
}

fn vector_mul_scalar_inplace(a: &mut [f32], scalar: f32) {
    a.iter_mut().for_each(|x| *x *= scalar);
}

fn kmeans_helper(a: &mut [f32], alpha: f32, beta: f32) {
    a.iter_mut().for_each(|x| *x = *x * alpha + beta);
}
