import apache_beam as beam

p2 = beam.Pipeline()


# lines = (
#             p2
#             |beam.Create(['Using create transform',
#                           'to generate memory in data',
#                           'This is 3rd Line',
#                           'Thanks'])
#             |beam.io.WriteToText('data/out_create1')
#
#
#
# )
# p2.run()

# lines = (
#             p2
#             |beam.Create([("Guilherme", 35),
#                           ("Baleia", 42),
#                           ("Sereia", 99),
#                           ("Tilápia", 42)])
#             |beam.io.WriteToText('data/out_create2')
#
#
#
# )
# p2.run()

lines = (
            p2
            |beam.Create({"arroz": [1, 2, 3],
                          "feijao": [1, 4, 6],
                          "banana": [0, 1, 0]})
            |beam.io.WriteToText('data/out_create4', num_shards=2)



)
p2.run()