#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info(f"Downloaded input artifact {args.input_artifact} to {artifact_local_path}")

    # Load the data from the artifact
    data = pd.read_csv(artifact_local_path)

    # Clean data
    cleaned_data = data[(data['price'] >= args.min_price) & (data['price'] <= args.max_price)]
    logger.info(f"Data cleaned. Rows before: {len(data)}, Rows after: {len(cleaned_data)}")

    # Drop rows in that are not in the proper geolocation
    idx = cleaned_data['longitude'].between(-74.25, -73.50) & cleaned_data['latitude'].between(40.5, 41.2)
    cleaned_data = cleaned_data[idx].copy()

    # Save the cleaned data to CSV
    cleaned_data.to_csv("clean_sample.csv", index=False)
    logger.info("Cleaned data saved to clean_sample.csv")

    # Upload cleaned data to W&B
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    logger.info(f"Uploaded cleaned data artifact {args.output_artifact}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact containing raw data",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact to store cleaned data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact (e.g., dataset)",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price threshold for data cleaning",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price threshold for data cleaning",
        required=True
    )


    args = parser.parse_args()

    go(args)
