echo "start marquez"
if [ -d "./marquez" ]
then
  echo "marquez directory already exists; not cloning"
else
  echo "marquez directory does not exist; cloning"
  git clone https://github.com/MarquezProject/marquez
fi
cd marquez
./docker/up.sh