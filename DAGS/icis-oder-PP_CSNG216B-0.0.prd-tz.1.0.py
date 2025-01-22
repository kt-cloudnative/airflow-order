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
                , WORKFLOW_NAME='PP_CSNG216B',WORKFLOW_ID='98dd498c0916427db303a5f8df3e6730', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG216B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('98dd498c0916427db303a5f8df3e6730')

    csng216bJob_vol = []
    csng216bJob_volMnt = []
    csng216bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng216bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng216bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng216bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c869785642d24f9585fd48360e144588',
        'volumes': csng216bJob_vol,
        'volume_mounts': csng216bJob_volMnt,
        'env_from':csng216bJob_env,
        'task_id':'csng216bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.46',
        'arguments':["--job.name=csng216bJob", "endTranDate=20240731", "empNo=91332365","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('98dd498c0916427db303a5f8df3e6730')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng216bJob,
        Complete
    ]) 

    # authCheck >> csng216bJob >> Complete
    workflow








