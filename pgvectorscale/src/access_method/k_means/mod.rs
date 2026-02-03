pub mod kmeans1d;
pub mod lloyd;
pub mod quick_centers;

use kmeans1d::kmeans1d;
use lloyd::LloydKMeans;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

pub fn k_means(
    c: usize,
    mut samples: Vec<Vec<f32>>,
    is_spherical: bool,
    iterations: usize,
    prefer_kmeanspp: bool,
) -> Vec<Vec<f32>> {
    assert!(c > 0);
    let n = samples.len();
    let dims = if n > 0 { samples[0].len() } else { 0 };
    assert!(dims > 0);

    if is_spherical {
        for sample in samples.iter_mut() {
            let norm = sample.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for value in sample.iter_mut() {
                    *value /= norm;
                }
            }
        }
    }

    if n <= c {
        return quick_centers::quick_centers(c, samples);
    }

    if dims == 1 {
        let flat_samples: Vec<f32> = samples.iter().flat_map(|v| v.iter().copied()).collect();
        let centroids = kmeans1d(c, &flat_samples);
        return centroids.into_iter().map(|c| vec![c]).collect();
    }

    let mut lloyd_k_means = LloydKMeans::new(c, samples, is_spherical, prefer_kmeanspp);
    for _ in 0..iterations {
        if lloyd_k_means.iterate() {
            break;
        }
    }
    lloyd_k_means.finish()
}

pub fn k_means_lookup(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    assert!(!centroids.is_empty());
    let mut result = (f32::INFINITY, 0);
    for (i, centroid) in centroids.iter().enumerate() {
        let dis = squared_distance(vector, centroid);
        if dis <= result.0 {
            result = (dis, i);
        }
    }
    result.1
}

pub fn k_means_lookup_many(vector: &[f32], centroids: &[Vec<f32>]) -> Vec<(f32, usize)> {
    assert!(!centroids.is_empty());
    let mut seq = Vec::new();
    for (i, centroid) in centroids.iter().enumerate() {
        let dis = squared_distance(vector, centroid);
        seq.push((dis, i));
    }
    seq
}

fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum()
}
