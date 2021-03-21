import apache_beam as beam


class ProcessWords(beam.DoFn):

    def process(self, element, cutoff_length, marker):

        name = element.split(',')[1]

        if len(name) <= cutoff_length:
            return [beam.pvalue.TaggedOutput('Short_Names', name)]

        else:
            return [beam.pvalue.TaggedOutput('Long_Names', name)]

        if name.startswith(marker):
            return name


p = beam.Pipeline()

results = (

            p
            |beam.io.ReadFromText('data/dept_data.txt')
            |beam.ParDo(ProcessWords(), cutoff_length=4, marker='A').with_outputs('Short_Names', 'Long_Names', main='Names_A')

)

short_collection = results.Short_Names
long_collection = results.Long_Names
startA_collection = results.Names_A

short_collection | "Write short" >> beam.io.WriteToText('data/short')
long_collection | "Write long" >> beam.io.WriteToText('data/long')
startA_collection | "Write startA" >> beam.io.WriteToText('data/startA')

p.run()