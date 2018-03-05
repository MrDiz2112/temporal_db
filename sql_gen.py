from gen_lib import create_database, rename_reference

tables = [
    ['table_name1', [
        ['row1', 'VARCHAR(50)'],
        ['row2', 'VARCHAR(50)']
    ]
     ],
     
    ['table_name2',
     [
         ['row1', 'INT NOT NULL REFERENCES Clients(id)'],
         ['row2', 'VARCHAR(50)'],
         ['row3', 'INT NOT NULL']
     ]
     ]
]

rename_reference(tables)

with open('temp.sql', 'w') as file:
    listing = create_database(tables)
    file.write(listing)
