from esBulk import EsBulk

def main():
    elasticNode = EsBulk("test_sample_index", "./targetFile/test_sample.json")
    elasticNode.ndJsonDocInsert()
if __name__ == "__main__":
    main()