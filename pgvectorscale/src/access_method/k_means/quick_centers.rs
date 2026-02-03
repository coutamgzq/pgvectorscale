use rand::Rng;

pub fn quick_centers(c: usize, samples: Vec<Vec<f32>>) -> Vec<Vec<f32>> {
    let n = samples.len();
    let dims = if n > 0 { samples[0].len() } else { 0 };
    assert!(c >= n);
    
    let mut rng = rand::thread_rng();
    let mut centroids = vec![vec![0.0f32; dims]; c];
    
    for centroid in centroids.iter_mut() {
        for value in centroid.iter_mut() {
            *value = rng.gen_range(0.0..1.0f32);
        }
    }
    
    for i in 0..n {
        centroids[i].copy_from_slice(&samples[i]);
    }
    
    centroids
}
