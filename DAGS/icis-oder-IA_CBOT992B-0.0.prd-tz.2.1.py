from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='IA_CBOT992B',WORKFLOW_ID='f1d87ea896b9472d9f70b63ef1b5cb1c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT992B-0.0.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f1d87ea896b9472d9f70b63ef1b5cb1c')

    cbot992bJob_vol = []
    cbot992bJob_volMnt = []
    cbot992bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot992bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot992bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot992bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot992bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot992bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cc7547b65a064c379dc1c7e2c50cc254',
        'volumes': cbot992bJob_vol,
        'volume_mounts': cbot992bJob_volMnt,
        'env_from':cbot992bJob_env,
        'task_id':'cbot992bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.7.1.5',
        'arguments':["--job.name=cbot992bJob", "requestDate="+str(datetime.now()), "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f1d87ea896b9472d9f70b63ef1b5cb1c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot992bJob,
        Complete
    ]) 

    # authCheck >> cbot992bJob >> Complete
    workflow








