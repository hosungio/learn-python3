from loguru import logger

A_1KB = "a" * 1024
B_1KB = "b" * 1024


if __name__ == "__main__":
    logger.add("/tmp/loguru_test/file_1.log")
    logger.add(
        "/tmp/loguru_test/file_rotation_1KB.log", rotation="1 KB", compression="zip"
    )

    logger.debug("debug log")
    logger.info("info log")
    logger.warning("warning log")
    logger.debug(A_1KB)
    logger.debug(B_1KB)
