

## points attributions
# customer couldn't pay at least 70% of value -> 1pt
# spent 100% of limit and did not clear the full amount -> 1pt
# both conditions for any month -> add 1 point
# sum all points and add to file

import apache_beam as beam
#
# class SplitRow(beam.DoFn):
#     def process(self, element):
#         # return type list
#         return [element.split(',')]
#
# class Spenter(beam.DoFn):
#     def process(self, element):
#         if (float(element[5]) - float(element[6])) > 0:
#             return [[element[0] + ',' + element[-1], 0]]
#         else:
#             if (float(element[8]) - float(element[6])) == 0:
#                 return [[element[0] + ',' + element[-1], 0]]
#             else:
#                 return [[element[0] + ',' + element[-1] , 1]]
#
#
# class Partial_paid(beam.DoFn):
#     def process(self, element):
#         if float(element[6]) == 0:
#             return [[element[0] + ',' + element[-1], 0]]
#         elif float(element[8])/float(element[6]) < .7:
#             return [[element[0] + ',' +element[-1], 1]]
#         else:
#             return [[element[0] + ',' + element[-1], 0]]
#
#
# with beam.Pipeline() as p:
#
#     base = (
#
#             p
#             |"read credit file" >> beam.io.ReadFromText('data/cards.txt', skip_header_lines=True)
#             |"split elements" >> beam.ParDo(SplitRow())
#     )
#
#     paid_month = (
#             base
#             |"return 1 if paid at least 0.7 spent" >> beam.ParDo(Partial_paid())
#             #|"write paid" >> beam.io.WriteToText('data/paid')
#     )
#
#     all_limit = (
#             base
#             |"return 1 if spent all limit and not paid full value" >> beam.ParDo(Spenter())
#             #|"write all" >> beam.io.WriteToText('data/all')
#     )
#
#     output = (
#
#              (paid_month, all_limit)
#              |beam.CoGroupByKey()
#              |beam.Map(lambda x: (x[0], x[1][0][0] + x[1][1][0]))
#              |beam.Map(lambda x: (x[0], x[1] + 1) if x[1] == 2 else x)
#              |beam.Map(lambda x: (x[0].split(',')[0], x[1]))
#              |beam.CombinePerKey(sum)
#              |beam.io.WriteToText('data/flatten4')
#
#     )




## course solution

p = beam.Pipeline()

def calculate_points(element):
    customer_id, first_name, last_name, relationship_no, \
    card_type, max_credit_limit, total_spent, \
    cash_withdrawn, cleared_amount, last_date = element.split(',')
    total_spent = int(total_spent)
    cleared_amount = int(cleared_amount)
    max_credit_limit = int(max_credit_limit)

    key_name = customer_id + ',' + first_name + ',' + last_name
    defaulter_points = 0

    if cleared_amount < 0.7*total_spent:
        defaulter_points += 1
    if (total_spent == max_credit_limit) and (cleared_amount < total_spent):
        defaulter_points += 1
    if (total_spent == max_credit_limit) and (cleared_amount < 0.7*total_spent):
        defaulter_points += 1
    return key_name, defaulter_points

def format_result(sum_points):
    key_name, points = sum_points
    return str(key_name) + ',' + str(points) + ' fraud points'




card_defaulter = (

            p
            |"Read data"              >> beam.io.ReadFromText('data/cards.txt', skip_header_lines=1)
            |"get poinst"             >> beam.Map(calculate_points)
            |"Group by sum"           >> beam.CombinePerKey(sum)
            |"Filter card defaulters" >> beam.Filter(lambda element: element[1]>0)
            |"Format output"          >> beam.Map(format_result)
            |"Save results"           >> beam.io.WriteToText('data/course_credit_results')


)

p.run()