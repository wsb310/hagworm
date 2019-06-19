
python3 setup.py sdist build
twine upload ./dist/*

rm -rf ./dist
rm -rf ./build
rm -rf ./hagworm.egg-info
