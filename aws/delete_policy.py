#!/usr/bin/env python

import boto3
import sys

iam = boto3.client("iam")


def filter_arn(s, xs):
    return list(filter(lambda arn: s in arn, xs))


def list_policies(s):
    try:
        policies = iam.list_policies().get("Policies", [])
        return filter_arn(s, [policy.get("Arn", "") for policy in policies])

    except Exception as ex:
        print(f"{ex}")
        return False


def get_version_id(version):
    return version.get("VersionId", "")


def get_version_ids(versions):
    return list(
        map(
            lambda version: get_version_id(version),
            versions,
        )
    )


def list_policy_versions(arn):
    return iam.list_policy_versions(PolicyArn=arn).get("Versions", [])


def delete_policy_version(arn, version_id):
    try:
        iam.delete_policy_version(PolicyArn=arn, VersionId=version_id)
        return True
    except Exception as ex:
        return False


def delete_policy_versions(policy_versions):
    try:
        for policy_version in policy_versions:
            print(f"[+] {policy_version}")
            for arn, versions in policy_version.items():
                for version_id in versions:
                    print(f"[+] {arn}: {version_id}")
                    delete_policy_version(arn=arn, version_id=version_id)
        return True
    except Exception as ex:
        return False


def delete_policy(arn):
    try:
        iam.delete_policy(PolicyArn=arn)
        return True
    except Exception as ex:
        return False


if len(sys.argv) > 1:
    policies = list_policies(sys.argv[1])

    if len(policies) > 0:
        print(f"[+] {policies}")
    else:
        print(f"[-] There is no policy matching the pattern: {sys.argv[1]}")
        sys.exit(0)

    policy_versions = [
        {arn: get_version_ids(list_policy_versions(arn))} for arn in policies
    ]

    print(f"[+] {policy_versions}")

    delete_policy_versions(policy_versions)

    for arn in policies:
        delete_policy(arn)

else:
    print(f"[-] Please specify a pattern")
