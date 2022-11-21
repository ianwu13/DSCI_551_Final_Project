exp_headers = [
    'SELECT EXAMPLE</br>FROM TMP</br>WHERE EXP = TRUE;',
    'SELECT co2_level</br>FROM co2_ppm.csv</br>WHERE year > A AND year < B'
]

exp_bodies = [
    'Example funciton description here some text too',
    'SECOND DESCRIPTION'
]

funct_forms = [
    ''.join([line.rstrip('\n') for line in open('pmr/forms/example_form.html', 'r').readlines()]),
    'PARAMETERS ARE A LOWER BOUND FLOAT VALUE AND UPPER BOUND FLOAT VALUE'
]