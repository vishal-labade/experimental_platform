from pathlib import Path
import urllib.request

JARS = [
  ("https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar", "hadoop-aws-3.3.4.jar"),
  ("https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar", "aws-java-sdk-bundle-1.12.262.jar"),
  ("https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.6.0/iceberg-spark-runtime-3.4_2.12-1.6.0.jar", "iceberg-spark-runtime-3.4_2.12-1.6.0.jar"),
  ("https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.0/iceberg-aws-bundle-1.6.0.jar", "iceberg-aws-bundle-1.6.0.jar"),
]

def main():
    out = Path("/opt/spark-jars")
    out.mkdir(parents=True, exist_ok=True)
    for url, name in JARS:
        dst = out / name
        if dst.exists():
            print("exists:", dst)
            continue
        print("downloading:", url)
        urllib.request.urlretrieve(url, dst)
        print("saved:", dst)

if __name__ == "__main__":
    main()
