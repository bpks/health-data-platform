"""Static cross-checks for infra/terraform that need no Terraform binary and no cloud access.

Checks: every .tf and .tfvars file parses; every var.x is declared and every variable is used;
every required variable is set in every environment file; every file() target exists; every
resource reference points at a declared resource. It complements, and does not replace,
`terraform validate` and `terraform plan`, which CI also runs.
"""

import pathlib
import re
import sys

import hcl2


def q(s: str) -> str:
    return s.strip('"')


tf = pathlib.Path("infra/terraform")
declared, required, res = {}, set(), set()
for p in tf.glob("*.tf"):
    d = hcl2.load(p.open())
    for v in d.get("variable", []):
        ((name, body),) = v.items()
        declared[q(name)] = body
        if "default" not in body:
            required.add(q(name))
    for r in d.get("resource", []):
        for rtype, inst in r.items():
            for name in inst:
                res.add(f"{q(rtype)}.{q(name)}")
text = "\n".join(p.read_text() for p in tf.glob("*.tf"))
used = set(re.findall(r"\bvar\.([a-z_]+)", text))
problems = []
problems += [f"var.{u} used but not declared" for u in sorted(used - set(declared))]
problems += [f"variable {d} declared but never used" for d in sorted(set(declared) - used)]
for env in ["dev", "test", "prod"]:
    vals = {k: v for k, v in hcl2.load((tf / "envs" / f"{env}.tfvars").open()).items() if k != "__comments__"}
    if required - set(vals):
        problems.append(f"{env}.tfvars missing required: {sorted(required - set(vals))}")
    if set(vals) - set(declared):
        problems.append(f"{env}.tfvars sets undeclared: {sorted(set(vals) - set(declared))}")
    print(
        f"{env}.tfvars: {len(vals)} values, all declared, all required set"
        if not any(env in x for x in problems)
        else f"{env}.tfvars: PROBLEM"
    )
for m in re.findall(r'file\("\$\{path\.module\}/([^"]+)"\)', text):
    if not (tf / m).resolve().exists():
        problems.append(f"file() target missing: {m}")
refs = set(re.findall(r"\b(azurerm_[a-z_]+\.[a-z_]+)\b", text)) - {"azurerm_client_config.current"}
problems += [f"reference to undeclared resource {r}" for r in sorted(refs - res)]
# validation: variable values used in tfvars satisfy simple constraints
for env in ["dev", "test", "prod"]:
    vals = hcl2.load((tf / "envs" / f"{env}.tfvars").open())
    assert q(vals["environment"]) == env, f"{env}.tfvars environment mismatch"
    assert re.fullmatch(r"\d{4}-\d{2}-01T00:00:00Z", q(vals["budget_start_date"])), "budget date"
print(
    f"variables: {len(declared)} declared / {len(required)} required / {len(used)} used"
    f" | resources: {len(res)} | references checked: {len(refs)}"
)
print("PROBLEMS:" if problems else "no cross-reference problems found")
[print(" -", x) for x in problems]
sys.exit(1 if problems else 0)
