variable "name" {
  type        = string
  description = "The name of the deployment"
  default     = "grove"
}

variable "image" {
  type        = string
  description = "Grove docker image to deploy"
  default     = "hashicorp/grove:latest"
}

variable "cpu" {
  type        = string
  default     = 256
  description = "The maximum amount of cpu, for the ecs task."
}

variable "memory" {
  type        = string
  default     = 512
  description = "The maximum amount of memory, in megabytes, for the ecs task."
}

variable "log_level" {
  type        = string
  default     = "INFO"
  description = "The log level of the function"

  validation {
    error_message = "The log level must be a valid Python logging name."

    # Taken from https://docs.python.org/3/library/logging.html#logging-levels
    condition = contains(
      [
        "CRITICAL",
        "ERROR",
        "WARNING",
        "INFO",
        "DEBUG",
      ],
      var.log_level,
    )
  }
}

variable "schedule" {
  type        = string
  description = "The CloudWatch schedule to invoke the Grove on"
  default     = "rate(15 minutes)"
}

variable "log_retention_in_days" {
  type        = number
  default     = 14
  description = "Number of days to retain logs in CloudWatch Logs"
}

