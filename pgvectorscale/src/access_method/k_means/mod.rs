pub mod kmeans1d;
pub mod lloyd;
pub mod quick_centers;

use kmeans1d::kmeans1d;
use lloyd::LloydKMeans;

/// k-means 聚类主函数
/// 
/// 该函数实现了 k-means 聚类算法，用于将向量数据分成 k 个聚类
/// 
/// 算法选择策略:
/// - 如果 is_spherical 为 true，先对向量进行 L2 归一化 (用于球面 k-means)
/// - 如果样本数量 n <= 聚类数 c，使用 quick_centers (每个样本作为一个中心)
/// - 如果维度 dims == 1，使用一维 k-means (kmeans1d)
/// - 否则使用标准的 Lloyd 算法 (多轮迭代直到收敛)
/// 
/// 参数说明:
/// - c: 聚类数量
/// - samples: 待聚类的向量集合，每个向量为 f32 数组
/// - is_spherical: 是否使用球面 k-means (先归一化)
/// - iterations: 最大迭代次数
/// - prefer_kmeanspp: 是否使用 k-means++ 初始化 (更好的初始中心点选择)
/// 
/// 返回值:
/// - Vec<Vec<f32>>: 聚类中心点坐标
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

/// 根据聚类中心点查找向量所属的聚类
/// 
/// 该函数计算输入向量与所有聚类中心点的距离，返回最近中心的索引
/// 
/// 实现原理:
/// - 遍历所有中心点，计算输入向量与每个中心的欧氏距离 (squared_distance)
/// - 使用平方距离避免开方运算，提高性能
/// - 返回距离最小的中心点索引
/// 
/// 参数说明:
/// - vector: 输入向量
/// - centroids: 聚类中心点集合
/// 
/// 返回值:
/// - usize: 最近的中心点索引 (即该向量所属的聚类 ID)
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

#[allow(dead_code)]
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
