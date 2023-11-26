old_file_path = 'soc-LiveJournal1.txt'
new_file_path = 'edges.txt'
vertices_path = 'vertices.txt'

with open(old_file_path, 'r') as old_file:
    old_content = old_file.readlines()

print('Number of lines in the file: ', len(old_content))
new_content = old_content[4:]

vertices = set()
for line in new_content:
    vertices.add(int(line.split('\t')[0]))
    vertices.add(int(line.split('\t')[1]))

print('Number of edges: ', len(new_content))
new_content.insert(0, 'src\tdst\n')
with open(new_file_path, 'w') as new_file:
    new_file.writelines(new_content)


vertices = list(vertices)
vertices.sort()
vertices = [str(vertex) + '\n' for vertex in vertices]
print('Number of vertices: ', len(vertices))
vertices.insert(0, 'id\n')
with open(vertices_path, 'w') as vertices_file:
    vertices_file.writelines(vertices)
