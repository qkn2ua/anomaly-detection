terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

locals {
  project_name = "ds5220-dp1-tf"


  key_name      = "ds5220-keypair"
  my_ip_cidr    = "67.129.8.209/32"
  repo_url      = "https://github.com/qkn2ua/anomaly-detection.git"
  ami_id        = "ami-0b6c6ebed2801a5cb"
  instance_type = "t3.micro"
}

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "${local.project_name}-bucket-"
}

resource "aws_sns_topic" "topic" {
  name = "ds5220-dp1"
}

resource "aws_sns_topic_policy" "topic_policy" {
  arn = aws_sns_topic.topic.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3PublishToTopic"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.topic.arn
        Condition = {
          StringEquals = {
            "AWS:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_role" "ec2_role" {
  name_prefix = "${local.project_name}-ec2-role-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "bucket_policy" {
  name = "${local.project_name}-bucket-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data_bucket.arn
      },
      {
        Sid    = "ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.data_bucket.arn}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name_prefix = "${local.project_name}-profile-"
  role        = aws_iam_role.ec2_role.name
}

resource "aws_security_group" "app_sg" {
  name_prefix = "${local.project_name}-sg-"
  description = "Allow SSH from my IP and FastAPI on port 8000"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_ip_cidr]
  }

  ingress {
    description = "FastAPI on 8000"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "app" {
  ami                         = local.ami_id
  instance_type               = local.instance_type
  key_name                    = local.key_name
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.app_sg.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = 16
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
              #!/bin/bash
              set -euxo pipefail

              exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

              apt-get update -y
              apt-get install -y git python3 python3-pip python3-venv curl

              export BUCKET_NAME="${aws_s3_bucket.data_bucket.bucket}"
              grep -q '^BUCKET_NAME=' /etc/environment || echo "BUCKET_NAME=${aws_s3_bucket.data_bucket.bucket}" >> /etc/environment

              rm -rf /opt/anomaly-detection
              git clone "${local.repo_url}" /opt/anomaly-detection
              chown -R ubuntu:ubuntu /opt/anomaly-detection

              python3 -m venv /opt/anomaly-detection/venv
              /opt/anomaly-detection/venv/bin/pip install --upgrade pip
              /opt/anomaly-detection/venv/bin/pip install -r /opt/anomaly-detection/requirements.txt

              cat > /etc/systemd/system/anomaly-api.service <<EOT
              [Unit]
              Description=Anomaly Detection FastAPI Service
              After=network.target

              [Service]
              Type=simple
              User=ubuntu
              WorkingDirectory=/opt/anomaly-detection
              Environment=BUCKET_NAME=${aws_s3_bucket.data_bucket.bucket}
              ExecStart=/opt/anomaly-detection/venv/bin/fastapi run /opt/anomaly-detection/app.py --host 0.0.0.0 --port 8000
              Restart=always
              RestartSec=5

              [Install]
              WantedBy=multi-user.target
              EOT

              systemctl daemon-reload
              systemctl enable anomaly-api
              systemctl start anomaly-api
              EOF

  tags = {
    Name = "${local.project_name}-anomaly-instance"
  }
}

resource "aws_eip" "app_eip" {
  domain   = "vpc"
  instance = aws_instance.app.id

  depends_on = [aws_instance.app]
}

resource "aws_sns_topic_subscription" "http_sub" {
  topic_arn = aws_sns_topic.topic.arn
  protocol  = "http"
  endpoint  = "http://${aws_eip.app_eip.public_ip}:8000/notify"

  depends_on = [aws_eip.app_eip]
}

resource "aws_s3_bucket_notification" "bucket_notify" {
  bucket = aws_s3_bucket.data_bucket.id

  topic {
    topic_arn     = aws_sns_topic.topic.arn
    events        = ["s3:ObjectCreated:Put"]
    filter_prefix = "raw/"
    filter_suffix = ".csv"
  }

  depends_on = [
    aws_sns_topic_policy.topic_policy,
    aws_sns_topic_subscription.http_sub
  ]
}

output "bucket_name" {
  description = "Name of the S3 bucket used by the application"
  value       = aws_s3_bucket.data_bucket.bucket
}

output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.app.id
}

output "elastic_ip_address" {
  description = "Elastic IP attached to the instance"
  value       = aws_eip.app_eip.public_ip
}

output "api_health_url" {
  description = "FastAPI health endpoint"
  value       = "http://${aws_eip.app_eip.public_ip}:8000/health"
}

output "api_docs_url" {
  description = "FastAPI docs endpoint"
  value       = "http://${aws_eip.app_eip.public_ip}:8000/docs"
}

output "sns_topic_arn" {
  description = "SNS topic ARN"
  value       = aws_sns_topic.topic.arn
}