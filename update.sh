VERSION=$1
if [ -z "$VERSION" ]
then
      echo "\$VERSION is empty. usage: update 1.0.0"
else
      echo "\$Updating with version $VERSION"
      echo $VERSION > VERSION
      git add .
      git commit -m "v$VERSION"
      git tag v$VERSION
      git push origin master
      git push origin v$VERSION
fi