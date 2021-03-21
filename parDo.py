import apache_beam as beam

class SplitRow(beam.DoFn):
    def process(self, element):
        # return type list
        return [element.split(',')]


with beam.Pipeline() as p1:
    attendance_count = (
            p1
             |"head from file" >> beam.io.ReadFromText('data/dept_data.txt', skip_header_lines=True)
             |"tokenize list" >> beam.ParDo(SplitRow())
             |"filtering" >> beam.ParDo(lambda x: [x] if x[-2] == 'Finance' else None)
             #|"filtering by area" >> beam.Filter(filtering)
             #|"agg 1 to each element" >> beam.Map(lambda record: (record[-1].split('-')[0], 1))
             #|"groupby key using sum" >> beam.CombinePerKey(sum)
             |"write file" >> beam.io.WriteToText('data/output_new_parDo')
    )
