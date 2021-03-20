import apache_beam as beam

def tokenize_text(text):
    return text.split(' ')

def remove_spaces(element):
    if element != " ":
        return element


p = beam.Pipeline()


pipeline = (

        p
        |"read from file" >> beam.io.ReadFromText('data/data.txt')
        |"tokenize data" >> beam.Map(tokenize_text)
        |"remove white spaces" >> beam.FlatMap(remove_spaces)
        #|"flatmap" >> beam.FlatMap()
        |"words assignment" >> beam.Map(lambda x: (x, 1))
        | "groupby key using sum for words" >> beam.CombinePerKey(sum)
        | "write words_count file" >> beam.io.WriteToText('data/words_count')


)
p.run()