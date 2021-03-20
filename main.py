import apache_beam as beam

def SplitRow(element):
    return element.split(',')

def filtering(record):
  return record[3] == 'HR'

#p1 = beam.Pipeline()
with beam.Pipeline() as p1:
    attendance_count = (
            p1
             |"head from file" >> beam.io.ReadFromText('data/dept_data.txt')
             |"tokenize list" >> beam.Map(SplitRow)
             |"filtering by area" >> beam.Filter(filtering)
             |"agg 1 to each element" >> beam.Map(lambda record: (record[-1].split('-')[0], 1))
             |"groupby key using sum" >> beam.CombinePerKey(sum)
             |"write file" >> beam.io.WriteToText('data/output_new_final_5')
    )

#p1.run()


