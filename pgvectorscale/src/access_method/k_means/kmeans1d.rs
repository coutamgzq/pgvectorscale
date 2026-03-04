pub fn kmeans1d(c: usize, a: &[f32]) -> Vec<f32> {
    assert!(0 < c && c < a.len());

    let mut a = a.to_vec();
    a.sort_by(|x, y| x.partial_cmp(y).unwrap());

    let n = a.len();

    if c == 1 {
        let mean = a.iter().sum::<f32>() / n as f32;
        return vec![mean];
    }

    let mut centroids = vec![0.0f32; c];

    let mut boundaries = vec![0usize; c + 1];
    boundaries[0] = 0;
    boundaries[c] = n;

    let chunk_size = n / c;
    for i in 1..c {
        boundaries[i] = i * chunk_size;
    }

    for _ in 0..100 {
        let mut new_boundaries = boundaries.clone();

        for i in 1..c {
            let left = boundaries[i - 1];
            let right = boundaries[i + 1];

            if right - left <= 1 {
                continue;
            }

            let mut best_cost = f32::INFINITY;
            let mut best_pos = boundaries[i];

            for j in (left + 1)..right {
                let cost_left = compute_cost(&a[left..j]);
                let cost_right = compute_cost(&a[j..right]);
                let total_cost = cost_left + cost_right;

                if total_cost < best_cost {
                    best_cost = total_cost;
                    best_pos = j;
                }
            }

            new_boundaries[i] = best_pos;
        }

        if new_boundaries == boundaries {
            break;
        }
        boundaries = new_boundaries;
    }

    for i in 0..c {
        let left = boundaries[i];
        let right = boundaries[i + 1];
        if left < right {
            centroids[i] = a[left..right].iter().sum::<f32>() / (right - left) as f32;
        } else {
            centroids[i] = a[left];
        }
    }

    centroids
}

fn compute_cost(a: &[f32]) -> f32 {
    if a.is_empty() {
        return 0.0;
    }

    let mean = a.iter().sum::<f32>() / a.len() as f32;
    a.iter().map(|x| (x - mean) * (x - mean)).sum()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sample_0() {
        let clusters = kmeans1d(
            4,
            &[
                -50.0, 4.0, 4.1, 4.2, 200.2, 200.4, 200.9, 80.0, 100.0, 102.0,
            ],
        );
        assert_eq!(clusters.len(), 4);
    }
}
