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
                , WORKFLOW_NAME='PP_CBON301B',WORKFLOW_ID='57115747fe294e4a91252c027f56d6a8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON301B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 21, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('57115747fe294e4a91252c027f56d6a8')

    cbon301bJob_vol = []
    cbon301bJob_volMnt = []
    cbon301bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon301bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon301bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon301bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon301bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon301bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2d5abce9ee274f94bd8761d039fe939f',
        'volumes': cbon301bJob_vol,
        'volume_mounts': cbon301bJob_volMnt,
        'env_from':cbon301bJob_env,
        'task_id':'cbon301bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cbon301bJob" , "endTranDate=20240528","ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('57115747fe294e4a91252c027f56d6a8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon301bJob,
        Complete
    ]) 

    # authCheck >> cbon301bJob >> Complete
    workflow








