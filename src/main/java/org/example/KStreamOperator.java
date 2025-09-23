package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class KStreamOperator {
    public static class Tuple {
        public String key;
        public String value;
        public Tuple() {}
        public Tuple(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class BF {
        private final BitSet bits;
        private final int size;
        private final int numHash;
        private final int[] seeds;

        public BF(int size, int numHash) {
            this.size = size;
            this.numHash = numHash;
            this.bits = new BitSet(size);
            int[] defaultSeeds = {7, 13, 17, 31, 37, 41, 61, 73};
            this.seeds = new int[numHash];
            for (int i = 0; i < numHash; i++) {
                this.seeds[i] = defaultSeeds[i % defaultSeeds.length];
            }
        }
        private int hash(String key, int seed) {
            int h = 0;
            for (char c : key.toCharArray()) {
                h = seed * h + c;
            }
            return Math.abs(h % size);
        }
        public boolean mightContain(String key) {
            for (int i = 0; i < numHash; i++) {
                if (!bits.get(hash(key, seeds[i]))) {
                    return false;
                }
            }
            return true;
        }
        public void put(String key) {
            for (int i = 0; i < numHash; i++) {
                bits.set(hash(key, seeds[i]));
            }
        }
    }
    public static class Bucket {
        public List<BF> bfVector;
        public List<int[]> indexVector;
        public Bucket(int bfSize, int numHash) {
            bfVector = new ArrayList<>();
            indexVector = new ArrayList<>();
            bfVector.add(new BF(bfSize, numHash));
            indexVector.add(new int[]{-1, -1});
        }
    }
    public static class KStream implements FlatMapFunction<Tuple, Tuple> {
        private final int n;
        private final double theta;
        private final int bfSize;
        private final int numHash;
        private final List<Bucket> buckets;
        private final int[] loads;
        public KStream(int n, double theta, int bfSize, int numHash) {
            this.n = n;
            this.theta = theta;
            this.bfSize = bfSize;
            this.numHash = numHash;
            this.buckets = new ArrayList<>();
            this.loads = new int[n];
            for (int i = 0; i < n; i++) {
                buckets.add(new Bucket(bfSize, numHash));
            }
        }
        private int hashKeyToBucket(String key) {
            return Math.abs(key.hashCode() % n);
        }
        private double computeLoadImbalance() {
            int max = Integer.MIN_VALUE;
            int sum = 0;
            for (int l : loads) {
                if (l > max) max = l;
                sum += l;
            }
            double avg = sum * 1.0 / n;
            return (max - avg) / max;
        }
        @Override
        public void flatMap(Tuple tuple, Collector<Tuple> out) throws Exception {
            int currentBucket = hashKeyToBucket(tuple.key);
            int bfIdx = 0;
            while (true) {
                Bucket bucket = buckets.get(currentBucket);
                BF bf = bucket.bfVector.get(bfIdx);
                int[] idx = bucket.indexVector.get(bfIdx);
                if (bf.mightContain(tuple.key)) {
                    loads[currentBucket]++;
                    bf.put(tuple.key);
                    out.collect(tuple);
                    break;
                } else {
                    if (idx[0] == -1) {
                        loads[currentBucket]++;
                        bf.put(tuple.key);
                        out.collect(tuple);
                        break;
                    } else {
                        currentBucket = idx[0];
                        bfIdx = idx[1];
                    }
                }
            }
            double imbalance = computeLoadImbalance();
            if (imbalance > theta) {
                int maxLoadInst = 0, minLoadInst = 0;
                int maxLoad = Integer.MIN_VALUE, minLoad = Integer.MAX_VALUE;
                for (int i = 0; i < n; i++) {
                    if (loads[i] > maxLoad) { maxLoad = loads[i]; maxLoadInst = i; }
                    if (loads[i] < minLoad) { minLoad = loads[i]; minLoadInst = i; }
                }
                Bucket maxBucket = buckets.get(maxLoadInst);
                Bucket minBucket = buckets.get(minLoadInst);
                int i2 = maxBucket.bfVector.size() - 1;
                int j2 = minBucket.bfVector.size() - 1;
                maxBucket.indexVector.set(i2, new int[]{minLoadInst, j2});
                maxBucket.bfVector.add(new BF(bfSize, numHash));
                maxBucket.indexVector.add(new int[]{-1, -1});
            }
        }
    }
}
