MNIST to Delta Lake Project 

1. Python version: 3.13
2. Java: OpenJDK 11 (Temurin)
3. Spark: 3.4.1 (Hadoop 3)
4. Delta Lake: 2.4.0 (Scala 2.12)
5. Packages: pyspark, pandas, pillow
6. Install: pip install -r requirements.txt

Folder structure:
- flat_files/mnistpng/training/0 to 9 (PNG images)
- scripts/
    - load_mnistimages.py
    - save_to_delta.py
    - view_table.py
    - My_Spark_Session.py
- image_data.pkl (auto-generated)
- delta_tables/mnist_digits/ (auto-created)
- requirements.txt
- README.txt

Run in this order from project root:
1. python scripts\load_mnistimages.py
2. python scripts\save_to_delta.py
3. spark-submit --packages io.delta:delta-core_2.12:2.4.0 scripts\view_table.py

Check delta_tables/mnist_digits/ to confirm table was created.


- Author: Nandini Pandey
