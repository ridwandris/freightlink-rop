# 1. IAM Role & Profile (Allows EC2 to talk to S3)
resource "aws_iam_role" "airflow_ec2_role" {
  name = "airflow_ec2_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "s3_full_access" {
  role       = aws_iam_role.airflow_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_instance_profile" "airflow_profile" {
  name = "airflow_ec2_profile"
  role = aws_iam_role.airflow_ec2_role.name
}

# 2. Security Group (Firewall Rules)
resource "aws_security_group" "airflow_sg" {
  name        = "airflow_security_group"
  description = "Allow SSH and Airflow UI traffic"

  # SSH Access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Note: In production, you would lock this to your specific IP
  }

  # Airflow Web UI Access
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound traffic (so the server can download Airflow and connect to SAP)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Fetch the latest Ubuntu 22.04 Image
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical's official AWS account ID

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# 3. The EC2 Instance (Free Tier with Swap Space Hack)
resource "aws_instance" "airflow_server" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.micro" 
  iam_instance_profile   = aws_iam_instance_profile.airflow_profile.name
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]

  root_block_device {
    volume_size = 30
    volume_type = "gp3" # gp3 is the newest, fastest, and cheapest tier
  }

  user_data = <<-EOF
              #!/bin/bash
              
              # --- 1. CREATE 4GB SWAP SPACE (FAKE RAM) ---
              fallocate -l 4G /swapfile
              chmod 600 /swapfile
              mkswap /swapfile
              swapon /swapfile
              echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab

              # --- 2. INSTALL DOCKER & COMPOSE ---
              apt-get update -y
              curl -fsSL https://get.docker.com -o get-docker.sh
              sh get-docker.sh
              apt-get install docker-compose-plugin -y
              
              usermod -aG docker ubuntu
              
              mkdir -p /home/ubuntu/airflow
              chown -R ubuntu:ubuntu /home/ubuntu/airflow
              EOF

  tags = {
    Name = "Airflow-Orchestrator"
  }
}

# Output the Public IP so we know where to connect
output "airflow_public_ip" {
  value       = aws_instance.airflow_server.public_ip
  description = "The public IP address of the Airflow EC2 instance"
}