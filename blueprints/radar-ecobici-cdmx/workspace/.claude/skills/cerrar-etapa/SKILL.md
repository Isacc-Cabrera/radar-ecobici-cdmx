---
name: cerrar-etapa
description: Cierra un paso o etapa de Radar ECOBICI - corre el gate, hace commit + tag del checkpoint, y entrega resumen, preguntas de checkpoint, entrada de bitácora y recordatorio de LinkedIn. Úsala cuando digas "terminé el paso", "cierra la etapa", "checkpoint" o "siguiente paso".
---

# Cerrar etapa

## Cuándo usarla
- Al terminar los comandos `Verify` de un paso de `blueprints/radar-ecobici-cdmx/tasks.json`.
- Cuando el usuario dice "cierra la etapa", "checkpoint", "ya quedó", "siguiente paso".

## Pasos
1. Corre todos los comandos `verify` del task en `tasks.json`, en orden, desde la raíz. Si uno
   falla, no sigas: arréglalo (nunca edites el comando).
2. Corre el gate: `uv run ruff check . && uv run ruff format --check . && uv run mypy && uv run pytest`.
3. Marca el task `done` en `blueprints/radar-ecobici-cdmx/tasks.json` (así el commit del
   checkpoint incluye el estado).
4. Ejecuta el bloque `Checkpoint` del epic tal cual (commit convencional en español con el id
   del task y `git tag step-NN-<slug>`), incluidas sus aserciones posteriores al commit.
5. Entrega al usuario, en este orden:
   - Resumen (5–8 viñetas) de lo construido y de los patrones de diseño usados.
   - Las 2–3 preguntas de checkpoint del epic ("Preguntas de checkpoint").
   - Propuesta de entrada para `docs/bitacora.md`: **qué decidí · alternativas · por qué ·
     consecuencias**. No la pegues tú: el usuario la revisa y la pega.
   - Recordatorio del post de LinkedIn si el epic lo indica para esta etapa.
6. Pregunta si hacer push (acción hacia afuera). Solo con un "sí" explícito corre:
   `git -c credential.helper= -c 'credential.helper=!f() { echo username=x-access-token; echo "password=$(gh auth token -u IsaccCabrera09)"; }; f' push --follow-tags`

## Verify
```bash
git tag -l 'step-*'          # expect: incluye el tag del paso recién cerrado
test -z "$(git status --porcelain)"   # expect: exit 0, árbol limpio después del commit
```

## No hagas
- No marques `done` con un gate rojo ni saltes un comando `verify`.
- No hagas push, `gh auth switch` ni `git config --global`.
- No escribas en `docs/bitacora.md` sin que el usuario apruebe el texto.
