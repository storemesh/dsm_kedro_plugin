# dsm-kedro-plugin

## Document

Open `docs/_build/html/index.html` in your browser

## Generate Documents from Sphinx 

### Set up sphinx docs 
**Important**: before generate docs, dsm_kedro_plugin need to be in dsm_kedro template to prevent import error
1. install sphinx and sphinx_rtd_theme
```sh
pip install sphinx and sphinx_rtd_theme
```

2. create docs folder in dsm_kedro_plugin
```sh
# in dsm_kedro_plugin
mkdir docs
cd docs
```

3. set up sphinx
```sh
sphinx-quickstart
```
use all default value except your project name and your name

4. edit `conf.py` by adding this

```python
import os
import sys
# this is build path when use 'make html' command. need to add all path to prevent importing error
sys.path.insert(0, os.path.abspath('../custom_dataset'))
sys.path.insert(0, os.path.abspath('../../..'))

...

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon'
]

...

html_theme = 'sphinx_rtd_theme' # change theme
...

autoclass_content = 'both' # to generate docs for __init__ in class
```

5. add "modules" to `index.rst`  the result will be like this
```
Welcome to DSM Kedro Plugin's documentation!
============================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
```

6. generate build paht for specific folder
```sh
sphinx-apidoc -o . ../custom_dataset
```

7. build html
```sh
make html
```

 Finish!!


### Update your docs
if your update your docstring in your source code, you can just rerun `make html` and the documents will be updated

### Ref
- tutorial https://www.youtube.com/watch?v=b4iFyrLQQh4   
- you can install sphinx in pip instead of apt install, it's also work
- you can copy config.py in here https://towardsdatascience.com/documenting-python-code-with-sphinx-554e1d6c4f6d 




## Validate Integration file with Data Product

ผมทำโค้ดเอาไว้เช็ค sheet data product กับ integration ไฟล์นะครับ ว่าคอลัมน์ไหนขาด, เกินหรือ data type ไม่ตรงบ้าง จะได้เช็คได้ง่ายๆครับว่าข้อมูลอันไหนยังไม่เรียบร้อย

โค้ดอยู่ใน dsm_kedro_plugin commit ล่าสุดนะครับ โปรเจคจะใช้ลองไป pull ลงมาได้

เงื่อนไขการใช้
1. catalog ของ integration ต้องอยู่ในไฟล์ conf/base/catalogs/manual/integration.yml ทั้งหมด
2. ต้องมีไฟล์ conf/local/service_account.json เพื่อให้ดึง google sheet ได้ (วิธี gen credential https://www.youtube.com/watch?v=bu5wXjz2KvU)

วิธีรัน
```sh
python src/dsm_kedro_plugin/check_data_product.py -gsheet_fname <ชื่อไฟล์ google sheet>

# ตัวอย่าง
# python src/dsm_kedro_plugin/check_data_product.py -gsheet_fname "DITP65_Integration Database Model"
```

report อยู่ที่ logs/check_data_product.log
