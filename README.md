  -This python module parse csv, txt or xml files in predefined schemas.
  -First you need to install few side libs: xmltodict- custom xml parser,
pymongo- MongoDB python client, pytest for testing. To do this you just 
need to run --> pip install -r requirements.txt.
  -To use this parser just run --> python parser.py <your file_to_parse.xml, .csv or .txt>. 
File to parse must be in the same directory with parser.py.
  -There are two awailable xml_parser_version, you can choose "1" or "2".
  -If you want to benchmark memory usage then uncomment @profile decorator 
of run_parser() function and comment run_benchmark() function at the end of file,
then run --> python -m memory_profiler parser.py <your file_to_parse.xml, .csv or .txt>
  -To run tests uncomment db_name = "pytest_db" and run --> 
python -m pytest -v pytest/test.py.
