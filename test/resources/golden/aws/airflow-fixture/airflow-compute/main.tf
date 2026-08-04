terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
}

resource "aws_vpc" "network" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "airflow" }
}

resource "aws_internet_gateway" "gateway" {
  vpc_id = aws_vpc.network.id
  tags   = { Name = "airflow" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.network.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "eu-west-1a"
  map_public_ip_on_launch = true
  tags                    = { Name = "airflow" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.network.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gateway.id
  }
  tags = { Name = "airflow" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "node1" {
  name   = "airflow"
  vpc_id = aws_vpc.network.id
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "airflow" }
}

resource "aws_key_pair" "operator" {
  key_name   = "airflow"
  public_key = file("/tmp/airflow.pub")
}

resource "aws_instance" "node1" {
  ami                         = "ami-example"
  instance_type               = "t3.small"
  availability_zone           = "eu-west-1a"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.node1.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.operator.key_name
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
  tags = { Name = "airflow" }
  connection {
    type  = "ssh"
    user  = "ubuntu"
    agent = true
    host  = self.public_ip
  }
  provisioner "remote-exec" {
    inline = ["ls"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

output "params" {
  value = {
    ip     = aws_instance.node1.public_ip
    sudoer = "ubuntu"
    uid    = "1000"
    name   = "airflow-fixture"
    user   = "ubuntu"
  }
}
