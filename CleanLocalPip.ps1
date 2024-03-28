pip freeze > backup_requirements.txt

pip freeze | xargs pip uninstall -y