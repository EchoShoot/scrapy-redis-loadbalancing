from sanic import Sanic,response
import asyncio
from sanic.response import json, text, html
from collections import namedtuple
import random
from jinja2 import Template

Config = namedtuple('Config', 'total display')  # 总数,每页显示数量
URL = namedtuple('URL', 'title link')

app = Sanic(log_config=False)

menu = {
	'image':{
		'live':Config(200,20),
		'dog':Config(300,20),
		'cat':Config(100,20),
		'girl':Config(500,20)
		},  
	'blog':{
		'python':Config(800,10),
		'php':Config(200,10),
		'C':Config(300,10),
		'Java':Config(1000,10),
		'JavaScript':Config(300,10)
		},
	'news':{
		'china':Config(700,30),
		'military':Config(500,30),
		'cite':Config(400,10),
		'variety':Config(300,30)
		},
	'finance':{
		'stock':Config(200,5),
		'petroleum':Config(50,5)
		},
}


# 后面会使用更方便的模板引用方式
template = Template(
	"""
	<!DOCTYPE html>
	<html lang="en">
	<head>
		<meta charset="UTF-8">
		<title>{{title}}</title>
		<meta name="viewport" content="width=device-width, initial-scale=1">
	</head>
	<body>
	<article class="markdown-body">
		<b><a href="/">Home</a></b> |
		{% for category in categorys %}
			{% if category.title == title %}
				<b><a href="{{category.link}}" style="color:red" >{{category.title}}</a></b> | 
			{% else %}
				<a href="{{category.link}}">{{category.title}}</a> | 
			{% endif %}
		{% endfor %}
	</article>
	<div class="nav">
		{% for page in pages %}
			{% if page.title == current_page %}
				<b><a href="{{page.link}}" style="color:green" >{{page.title}}</a></b> | 
			{% else %}
				<a href="{{page.link}}">{{page.title}}</a> |
			{% endif %}
		{% endfor %}
	</div>
	<hr>
	<div class="content">
		{% if top and bottom %}
			<a href="{{top.link}}">[1]</a>
			<span>...</span>
			{% for content in contents %}
				<a href="{{content.link}}">{{content.title}}</a>|
			{% endfor %}
			<span>...</span>
			<a href="{{bottom.link}}">[{{bottom.title}}]</a>
		{% endif %}
	</div>
	<hr/>
	</body>
	</html>
	"""
)


@app.route("/")
async def index(request):
	categorys = [URL(title=title,link='/{}'.format(title)) for title in menu.keys()]
	return response.redirect(categorys[0].link)

@app.route("/<category>")
async def page(request, category):
	if menu.get(category):
		pages = [URL(title=page,link='/{}/{}'.format(category, page)) for page in menu.get(category)]
		return response.redirect(pages[0].link)

@app.route("/<category>/<page>")
async def page(request, category, page):
	pages = [URL(title=page,link='/{}/{}'.format(category, page)) for page in menu.get(category)]
	return response.redirect('/{}/{}/1'.format(category, page))

@app.route("/<category>/<page>/<content:int>")
async def page(request, category, page, content):
	UA = request.headers.get('user-agent')
	print(UA)
#	if len(UA) < 20:
#		await asyncio.sleep(float(UA))
	categorys = [URL(title=title,link='/{}'.format(title)) for title in menu.keys()]
	category_config = menu.get(category)
	pages = [URL(title=page,link='/{}/{}'.format(category, page)) for page in category_config]
	page_config = category_config.get(page)

	_top = 1 # 首页
	top = URL(title=_top,link='/{}/{}/{}'.format(category, page, _top))
	_bottom = page_config.total  # 末页
	bottom = URL(title=_bottom,link='/{}/{}/{}'.format(category, page, _bottom))
	half = page_config.display//2
	
	if content <= half:
		_from = _top
	elif half < content < _bottom - half:
		_from = content - page_config.display//2
	elif content >= _bottom - half:
		_from = _bottom - page_config.display
	_to = _from + page_config.display
	_display = range(_from,_to)
	
	contents = [URL(title=content,link='/{}/{}/{}'.format(category, page, content)) for content in _display]
	html_content = template.render(title=category,categorys=categorys,
								current_page=page,pages=pages,contents=contents,
								top=top,bottom=bottom)
	return html(html_content)


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=8000)