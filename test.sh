python3 -m pip install -r requirements.txt
python3 -m pip install pytest pytest-cov==2.5.1 pytest-pep8 coveralls

python3 -m pytest --cov=autoscaler --pep8 autoscaler

