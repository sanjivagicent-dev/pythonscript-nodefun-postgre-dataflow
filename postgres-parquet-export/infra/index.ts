import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

// IAM Role for Lambda
const role = new aws.iam.Role("lambdaRole", {
    assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({
        Service: "lambda.amazonaws.com",
    }),
});

new aws.iam.RolePolicyAttachment("lambdaPolicy", {
    role: role.name,
policyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
});

// Point to your compiled JS (IMPORTANT)
const codeArchive = new pulumi.asset.AssetArchive({
    ".": new pulumi.asset.FileArchive("../dist"),
});

// Your handlers
const functions = [
    { name: "mainLambda", handler: "handler:handler/lambdaHandler.handler" },
    // { name: "orderLambda", handler: "handlers/order.handler" },
];

functions.forEach(fn => {
    new aws.lambda.Function(fn.name, {
        runtime: "nodejs18.x",
        handler: fn.handler,
        role: role.arn,
        code: codeArchive,
        timeout: 10,
        environment: {
            variables: {
                DATABASE_URL: "postgresql://postgres:postgres@localhost:5433/oilfield", // update this
            },
        },
    });
});