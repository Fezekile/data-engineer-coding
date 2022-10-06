
#=======setting up aws credentials            
provider "aws" {
    region = "${var.aws_region}"
    access_key = "${var.aws_access_key}"
    secrete_key = "${var.aws_secrete_key}"
}


##################
# Glue Crawler   #
##################

resource "aws_glue_crawler" "aws_glue_crawler_ODS" {
    name          = "${var.name}"
    database_name = "${var.db}"
    role          = "${var.role}"
    jdbc_target {
        connection_name = "${var.connection_name}"
        path            = "${var.crawler_role}"
        PASSWORD            = "${var.PG_PASSWORD}"
        USERNAME            = "${var.PG_USERNAME}"
    }
}
###################
# Glue Catalog   #
##################
resource "aws_glue_catalog" "glue_database" {
  count = "${var.create ? 1 : 0}"

  name = "${var.name}"

  description  = "${var.description}"
  catalog_id   = "${var.catalog}"
  location_uri = "${var.location_uri}"
  parameters   = "${var.params}"
}



##################
# Glue Job       #
##################

#======starting by creating the s3 bucket that will store the script========

resource "aws_s3_bucket_object" "upload-glue-script" {
    bucket = "${var.bucket-name}"
    key = "${var.file-name}"
    source = "${var.file-name}"
  
}

resource "aws_glue_job" "glueJob" {
  name     = "${var.glue-job-name}"
  role_arn = "${var.glue_job_role_arn}"
  description = "This is script to extract and aggregate the data"
  max_retries = "1"
  timeout = 2880

  command {
    script_location = "s3://${var.bucket-name}/glue/${var.file-name}"
    python_version = "3"
  }
  execution_property {
    max_concurrent_runs = 2
  }
  glue_version = "3.0"
}

##################
# Creat S3 bucket #
##################

module "s3" {

    source = "bucket_for_aggregated_data"

    bucket_name = "my_bucket"       

}

resource "aws_s3_bucket" "ods_files" {

    bucket = "${var.bucket_name}" 

    acl = "${var.acl_value}"   

}


