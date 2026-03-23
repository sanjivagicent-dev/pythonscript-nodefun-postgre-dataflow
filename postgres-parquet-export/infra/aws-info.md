# AWS Credentials Setup for Pulumi (Local Development)

## 📌 Why AWS Credentials Are Stored Outside the Project

### 🔐 1. Security

Storing credentials inside your project is risky:

* They can accidentally be committed to version control (e.g., Git)
* Anyone with access can use your AWS account
* This can lead to unexpected costs and security issues

✅ Keeping credentials outside the project reduces this risk.

---

### 🧑‍💻 2. Reusability Across Projects

You may have multiple projects on your machine:

```
project1/
project2/
project3/
```

Instead of duplicating credentials in each project, a single configuration works for all.

---

### ⚙️ 3. Standard Practice

Most tools automatically look for credentials in:

```
~/.aws/credentials
~/.aws/config
```

Tools that use this:

* Pulumi
* AWS CLI
* AWS SDKs

---

### 🔄 4. Easy Environment Switching

You can define multiple profiles:

```ini
[default]
aws_access_key_id=AAA

[dev]
aws_access_key_id=BBB

[prod]
aws_access_key_id=CCC
```

Switch environment using:

```bash
export AWS_PROFILE=dev
```

---

### 🧠 5. Clean Separation

| Type             | Location |
| ---------------- | -------- |
| Application Code | project/ |
| Infra Code       | infra/   |
| Credentials      | ~/.aws/  |

---

## 📁 Correct File Paths (Linux)

### Credentials File

```
/home/<your-username>/.aws/credentials
```

Example:

```ini
[default]
aws_access_key_id=YOUR_ACCESS_KEY
aws_secret_access_key=YOUR_SECRET_KEY
```

---

### Config File

```
/home/<your-username>/.aws/config
```

Example:

```ini
[default]
region=ap-south-1
output=json
```

---

## ⚙️ How to Create These Files

### Create directory

```bash
mkdir -p ~/.aws
```

### Create credentials file

```bash
nano ~/.aws/credentials
```

### Create config file

```bash
nano ~/.aws/config
```

---

## 🧪 Verify Setup

Run:

```bash
aws sts get-caller-identity
```

If it returns account details → setup is correct ✅

---

## 🚀 Deployment Flow

```bash
# Build project
npm run build

# Deploy infra
cd infra
pulumi up
```

---

## ❌ What NOT to Do

### Do NOT store credentials in project files:

```js
// ❌ Never do this
const awsKey = "ABC123";
```

### Do NOT commit credentials to Git

---

## 🎯 Summary

* Store AWS credentials in `~/.aws/`
* Do not keep them inside your project
* Pulumi automatically uses these credentials
* This is the standard, secure, and scalable approach

---

## 💡 Analogy

Think of AWS credentials like your ATM card:

* You don’t store it inside every app
* You keep it safe and use it when needed

---

**You’re now ready to securely deploy with Pulumi 🚀**
