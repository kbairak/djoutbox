from django.http import JsonResponse
from django.shortcuts import render

from djoutbox import publish
from myapp.types import Payload


def index(request):
    if request.method == "POST":
        data = Payload.model_validate_json(request.body)
        publish(data.routing_key, data)
        return JsonResponse({"status": "published", "routing_key": data.routing_key})

    return render(request, "myapp/index.html")
