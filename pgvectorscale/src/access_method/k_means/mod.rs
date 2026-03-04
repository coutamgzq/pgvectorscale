pub mod kmeans1d;
pub mod lloyd;
pub mod quick_centers;

use kmeans1d::kmeans1d;
use lloyd::LloydKMeans;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

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
        let dis = squared_distance_optimized(vector, centroid);
        if dis <= result.0 {
            result = (dis, i);
        }
    }
    result.1
}

#[allow(dead_code)]
pub fn k_means_lookup_many(vector: &[f32], centroids: &[Vec<f32>]) -> Vec<(f32, usize)> {
    assert!(!centroids.is_empty());
    let mut seq = Vec::new();
    for (i, centroid) in centroids.iter().enumerate() {
        let dis = squared_distance_optimized(vector, centroid);
        seq.push((dis, i));
    }
    seq
}

#[inline]
fn squared_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn squared_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len();
    let chunks = len / 8;

    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        let diff = _mm256_sub_ps(va, vb);
        acc = _mm256_fmadd_ps(diff, diff, acc);
    }

    let mut result = [0.0f32; 8];
    _mm256_storeu_ps(result.as_mut_ptr(), acc);
    let mut sum: f32 = result.iter().sum();

    for i in (chunks * 8)..len {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
#[inline]
unsafe fn squared_distance_avx(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len();
    let chunks = len / 8;

    let mut acc = _mm256_setzero_ps();
    for i in 0..chunks {
        let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        let diff = _mm256_sub_ps(va, vb);
        let squared = _mm256_mul_ps(diff, diff);
        acc = _mm256_add_ps(acc, squared);
    }

    let mut result = [0.0f32; 8];
    _mm256_storeu_ps(result.as_mut_ptr(), acc);
    let mut sum: f32 = result.iter().sum();

    for i in (chunks * 8)..len {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
unsafe fn squared_distance_sse2(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len();
    let chunks = len / 4;

    let mut acc = _mm_setzero_ps();
    for i in 0..chunks {
        let va = _mm_loadu_ps(a.as_ptr().add(i * 4));
        let vb = _mm_loadu_ps(b.as_ptr().add(i * 4));
        let diff = _mm_sub_ps(va, vb);
        let squared = _mm_mul_ps(diff, diff);
        acc = _mm_add_ps(acc, squared);
    }

    let mut result = [0.0f32; 4];
    _mm_storeu_ps(result.as_mut_ptr(), acc);
    let mut sum: f32 = result.iter().sum();

    for i in (chunks * 4)..len {
        let diff = a[i] - b[i];
        sum += diff * diff;
    }

    sum
}

#[inline]
pub fn squared_distance_optimized(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { squared_distance_avx2(a, b) };
        }
        if is_x86_feature_detected!("avx") {
            return unsafe { squared_distance_avx(a, b) };
        }
        if is_x86_feature_detected!("sse2") {
            return unsafe { squared_distance_sse2(a, b) };
        }
    }
    squared_distance_scalar(a, b)
}

#[inline]
pub fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    squared_distance_optimized(a, b)
}
