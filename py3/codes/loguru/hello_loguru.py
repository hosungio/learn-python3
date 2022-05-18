from loguru import logger


if __name__ == "__main__":
    logger.debug("hello debug")
    logger.info("hello info")
    logger.warning("hello warning")
    
    try:
        raise Exception("hello exception")
    except Exception as ex:
        logger.error(f"hello error: {ex}")

    try:
        raise Exception("world exception")
    except Exception as ex:
        logger.opt(exception=True).error("print stack trace")
