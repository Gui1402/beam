import apache_beam as beam


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0) #initialize sum and count
    def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum+input, count+1
    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators) #[(9, 2), (10, 3)] -> [(9, 10), (3, 5)]
        return sum(ind_sums), sum(ind_counts)
    def extract_output(self, sum_count):
        sum, count = sum_count
        return sum/count if count else float('NaN')
p = beam.Pipeline()

small_sum = (
                p
                |beam.Create([10, 9, 8, 5, 5, 7, 12])
                |beam.CombineGlobally(AverageFn())
                |beam.io.WriteToText('data/combine')


)

p.run()