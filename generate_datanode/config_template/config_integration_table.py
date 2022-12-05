#### Template ####
# integration_table =  {
#     "<your integration table 1>": None, 
#     '<your integration table 2>': None, 
#     '<your integration table 3>': {
#         'columns': {
#             'id': { 'data_type': 'string', 'is_required': True},
#             'title': { 'data_type': 'string', 'is_required': True},
#             'unit': { 'data_type': 'string', 'is_required': True},
#             'description': { 'data_type': 'string', 'is_required': False},
#             'unit_price': { 'data_type': 'float', 'is_required': True, 'validation_rule': [1]},
#         },
#         'pk_column': 'id',
#         'validation_rule': [1],   # for rows validation
#     }, 
# }

integration_table =  {
    'Profile': None, 
    'Order': None, 
    'Orderitem': None, 
    'Item': None, 
    'Payment': None,
}