# import apache_beam as beam
#
# def SplitRow(element):
#     return element.split(',')
#
# def filtering(record):
#   return record[3] == 'HR'
#
# p1 = beam.Pipeline()
#
# input_collection = (
#
#              p1
#              |"head from file" >> beam.io.ReadFromText('data/dept_data.txt')
#              |"tokenize list" >> beam.Map(SplitRow)
# )
#
# accounts_count = (
#
#               input_collection
#               |"filtering by accounts" >> beam.Filter(lambda elements: elements[3] == 'Accounts')
#               |"agg 1 to each element for accounts" >> beam.Map(lambda record: (record[1], 1))
#               |"groupby key using sum for accounts" >> beam.CombinePerKey(sum)
#               |"write accounts file" >> beam.io.WriteToText('data/output_accounts_count')
# )
#
# accounts_count/ = (
#
#               input_collection
#               |"filtering by hr" >> beam.Filter(lambda elements: elements[3] == 'HR')
#               |"agg 1 to each element for hr" >> beam.Map(lambda record: (record[1], 1))
#               |"groupby key using sum for hr" >> beam.CombinePerKey(sum)
#               |"write hr file" >> beam.io.WriteToText('data/output_hr_count')
# )
#
#
#
#
# p1.run()
#
#



import apache_beam as beam

def SplitRow(element):
    return element.split(',')

def filtering(record):
  return record[3] == 'HR'

p1 = beam.Pipeline()

input_collection = (

             p1
             |"head from file" >> beam.io.ReadFromText('data/dept_data.txt')
             |"tokenize list" >> beam.Map(SplitRow)
)

accounts_count = (

              input_collection
              |"filtering by accounts" >> beam.Filter(lambda elements: elements[3] == 'Accounts')
              |"agg 1 to each element for accounts" >> beam.Map(lambda record: ('Accounts ' + record[1], 1))
              |"groupby key using sum for accounts" >> beam.CombinePerKey(sum)
              #|"write accounts file" >> beam.io.WriteToText('data/output_accounts_count')
)

hr_count = (

              input_collection
              |"filtering by hr" >> beam.Filter(lambda elements: elements[3] == 'HR')
              |"agg 1 to each element for hr" >> beam.Map(lambda record: ('HR ' + record[1], 1))
              |"groupby key using sum for hr" >> beam.CombinePerKey(sum)
              #|"write hr file" >> beam.io.WriteToText('data/output_hr_count')
)

output = (

    (accounts_count, hr_count)
    |beam.Flatten()
    |beam.io.WriteToText('data/flatten4')

)


p1.run()


