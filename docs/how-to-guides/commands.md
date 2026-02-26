# Refresh applicationset

```shell
kubectl -n argocd annotate applicationset restore-prive argocd.argoproj.io/refresh=hard --overwrite
```